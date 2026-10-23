package heritage.ledger;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

/**
 * In-memory mock of the heritage ledger.
 *
 * <p>Holds move from {@code held} to either {@code committed} or {@code released}. Each ledger endpoint
 * (hold, commit, release) has its own fault switches and call stats. All state is guarded by one lock;
 * the simulated latency happens outside the lock so concurrent calls overlap and show up in
 * {@code in_flight}.
 */
public final class Ledger {

    /** The three ledger endpoints. The lowercase name is the key used in the admin API. */
    public enum Service {
        HOLD, COMMIT, RELEASE;

        public String key() {
            return name().toLowerCase();
        }

        static Service parse(Object value) {
            if (value instanceof String s) {
                for (Service service : values()) {
                    if (service.key().equals(s)) {
                        return service;
                    }
                }
            }
            return null;
        }
    }

    /** An HTTP-style result: status code and JSON body. */
    public record Result(int status, Map<String, Object> body) {
        static Result ok(Map<String, Object> body) {
            return new Result(200, body);
        }

        static Result error(int status, String message) {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("error", message);
            return new Result(status, body);
        }

        public boolean isOk() {
            return status >= 200 && status < 300;
        }
    }

    private static final class Hold {
        final String holdId;
        final String requestId;
        final Object amount;
        final String createdAt;
        String status = "held";

        Hold(String holdId, String requestId, Object amount, String createdAt) {
            this.holdId = holdId;
            this.requestId = requestId;
            this.amount = amount;
            this.createdAt = createdAt;
        }

        Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("hold_id", holdId);
            m.put("request_id", requestId);
            m.put("amount", amount);
            m.put("status", status);
            m.put("created_at", createdAt);
            return m;
        }
    }

    private static final class Faults {
        int failNext;
        int rejectNext;
        int delayMs;

        Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("fail_next", failNext);
            m.put("reject_next", rejectNext);
            m.put("delay_ms", delayMs);
            return m;
        }
    }

    private static final class Stats {
        long calls;
        long ok;
        long errors;
        long inFlight;
        long maxInFlight;

        Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("calls", calls);
            m.put("ok", ok);
            m.put("errors", errors);
            m.put("rejected_429", 0L);
            m.put("in_flight", inFlight);
            m.put("max_in_flight", maxInFlight);
            return m;
        }
    }

    private final Object lock = new Object();
    private final long baseLatencyMs;

    // Guarded by lock.
    private final Map<String, Hold> holds = new LinkedHashMap<>();
    private final Map<String, Integer> holdCounters = new HashMap<>();
    private final Map<Service, Faults> faults = new EnumMap<>(Service.class);
    private final Map<Service, Stats> stats = new EnumMap<>(Service.class);

    /**
     * @param baseLatencyMs simulated latency per ledger call. A value above 0 gets a small random jitter.
     */
    public Ledger(long baseLatencyMs) {
        this.baseLatencyMs = baseLatencyMs;
        resetLocked();
    }

    // ---- Ledger endpoints ----

    /** POST /ledger/hold. Idempotent per request_id: returns the open hold if there is one. */
    public Result hold(Map<String, Object> request) {
        return call(Service.HOLD, () -> {
            Object requestId = request.get("request_id");
            Object amount = request.get("amount");
            if (!(requestId instanceof String rid) || rid.isBlank()) {
                return Result.error(400, "request_id is required");
            }
            if (!(amount instanceof Number n) || n.doubleValue() < 0) {
                return Result.error(400, "amount must be a non-negative number");
            }
            synchronized (lock) {
                for (Hold h : holds.values()) {
                    if (h.requestId.equals(rid) && h.status.equals("held")) {
                        return Result.ok(statusBody(h));
                    }
                }
                int seq = holdCounters.merge(rid, 1, Integer::sum);
                String holdId = "H-" + rid + "-" + seq;
                Hold h = new Hold(holdId, rid, amount, Instant.now().truncatedTo(ChronoUnit.MILLIS).toString());
                holds.put(holdId, h);
                return Result.ok(statusBody(h));
            }
        });
    }

    /** POST /ledger/commit. 404 for an unknown hold, 409 if released, idempotent if already committed. */
    public Result commit(Map<String, Object> request) {
        return call(Service.COMMIT, () -> transition(request, "committed", "released"));
    }

    /** POST /ledger/release. 404 for an unknown hold, 409 if committed, idempotent if already released. */
    public Result release(Map<String, Object> request) {
        return call(Service.RELEASE, () -> transition(request, "released", "committed"));
    }

    private Result transition(Map<String, Object> request, String target, String conflicting) {
        Object holdIdValue = request.get("hold_id");
        if (!(holdIdValue instanceof String holdId) || holdId.isBlank()) {
            return Result.error(400, "hold_id is required");
        }
        synchronized (lock) {
            Hold h = holds.get(holdId);
            if (h == null) {
                return Result.error(404, "unknown hold: " + holdId);
            }
            if (h.status.equals(conflicting)) {
                return Result.error(409, "hold " + holdId + " is already " + conflicting);
            }
            h.status = target;
            return Result.ok(statusBody(h));
        }
    }

    /** GET /ledger/holds. */
    public Map<String, Object> holds() {
        synchronized (lock) {
            List<Map<String, Object>> list = new ArrayList<>();
            for (Hold h : holds.values()) {
                list.add(h.toMap());
            }
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("holds", list);
            return body;
        }
    }

    // ---- Admin endpoints ----

    /** GET /admin/stats. */
    public Map<String, Object> stats() {
        synchronized (lock) {
            Map<String, Object> body = new LinkedHashMap<>();
            for (Service s : Service.values()) {
                body.put(s.key(), stats.get(s).toMap());
            }
            long open = 0, committed = 0, released = 0;
            for (Hold h : holds.values()) {
                switch (h.status) {
                    case "held" -> open++;
                    case "committed" -> committed++;
                    case "released" -> released++;
                    default -> { }
                }
            }
            body.put("open_holds", open);
            body.put("committed", committed);
            body.put("released", released);
            return body;
        }
    }

    /** POST /admin/faults. */
    public Result setFault(Map<String, Object> request) {
        Service service = Service.parse(request.get("service"));
        if (service == null) {
            return Result.error(400, "service must be one of hold, commit, release");
        }
        Object modeValue = request.get("mode");
        if (!(modeValue instanceof String mode)) {
            return Result.error(400, "mode is required");
        }
        int count = intField(request, "count", 1);
        int delayMs = intField(request, "delay_ms", 0);
        if (count < 0 || delayMs < 0) {
            return Result.error(400, "count and delay_ms must be non-negative");
        }
        synchronized (lock) {
            Faults f = faults.get(service);
            switch (mode) {
                case "fail_next" -> f.failNext = count;
                case "reject_next" -> f.rejectNext = count;
                case "slow" -> f.delayMs = delayMs;
                case "clear" -> faults.put(service, new Faults());
                default -> {
                    return Result.error(400, "mode must be one of fail_next, reject_next, slow, clear");
                }
            }
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("ok", true);
            body.put("faults", faultsViewLocked());
            return Result.ok(body);
        }
    }

    /** The current fault switches, keyed by service. */
    public Map<String, Object> faults() {
        synchronized (lock) {
            return faultsViewLocked();
        }
    }

    /** POST /admin/reset. Clears holds, faults and stats. */
    public void reset() {
        synchronized (lock) {
            resetLocked();
        }
    }

    // ---- Internals ----

    private Result call(Service service, Supplier<Result> logic) {
        long delayMs;
        synchronized (lock) {
            Stats s = stats.get(service);
            s.calls++;
            s.inFlight++;
            s.maxInFlight = Math.max(s.maxInFlight, s.inFlight);
            delayMs = latencyMs() + faults.get(service).delayMs;
        }
        Result result;
        try {
            if (delayMs > 0) {
                Thread.sleep(delayMs);
            }
            Result injected = consumeFault(service);
            result = injected != null ? injected : logic.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            result = Result.error(503, "interrupted");
        } catch (RuntimeException e) {
            result = Result.error(500, "internal error: " + e.getMessage());
        }
        synchronized (lock) {
            Stats s = stats.get(service);
            s.inFlight--;
            if (result.isOk()) {
                s.ok++;
            } else {
                s.errors++;
            }
        }
        return result;
    }

    private Result consumeFault(Service service) {
        synchronized (lock) {
            Faults f = faults.get(service);
            if (f.failNext > 0) {
                f.failNext--;
                return Result.error(503, "ledger " + service.key() + " unavailable (injected fault)");
            }
            if (f.rejectNext > 0) {
                f.rejectNext--;
                return Result.error(422, "ledger rejected " + service.key() + " (injected business rejection)");
            }
            return null;
        }
    }

    private long latencyMs() {
        if (baseLatencyMs <= 0) {
            return 0;
        }
        long jitter = Math.max(1, baseLatencyMs / 5);
        return baseLatencyMs + ThreadLocalRandom.current().nextLong(-jitter, jitter + 1);
    }

    private Map<String, Object> faultsViewLocked() {
        Map<String, Object> view = new LinkedHashMap<>();
        for (Service s : Service.values()) {
            view.put(s.key(), faults.get(s).toMap());
        }
        return view;
    }

    private void resetLocked() {
        holds.clear();
        holdCounters.clear();
        for (Service s : Service.values()) {
            faults.put(s, new Faults());
            // Keep in_flight accurate for calls that are running during a reset.
            Stats old = stats.get(s);
            Stats fresh = new Stats();
            if (old != null) {
                fresh.inFlight = old.inFlight;
                fresh.maxInFlight = old.inFlight;
            }
            stats.put(s, fresh);
        }
    }

    private static Map<String, Object> statusBody(Hold h) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("hold_id", h.holdId);
        m.put("status", h.status);
        return m;
    }

    private static int intField(Map<String, Object> request, String name, int fallback) {
        Object v = request.get(name);
        return v instanceof Number n ? n.intValue() : fallback;
    }
}
