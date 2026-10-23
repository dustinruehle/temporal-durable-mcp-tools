package heritage.ledger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityInfo;
import io.temporal.failure.ApplicationFailure;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ledger Activities. Each one is a plain HTTP call to the ledger API, so the Temporal path and a direct
 * caller hit the same service. HTTP outcomes map to Temporal failures:
 *
 * <ul>
 *   <li>503, other 5xx, or a connection error: retryable, type {@code LedgerUnavailable}
 *   <li>422: non-retryable, type {@code LedgerRejected}
 *   <li>404: non-retryable, type {@code LedgerNotFound}
 *   <li>409: non-retryable, type {@code LedgerConflict}
 *   <li>other 4xx: non-retryable, type {@code LedgerBadRequest}
 * </ul>
 */
public final class LedgerActivitiesImpl implements LedgerActivities {

    private static final Logger log = LoggerFactory.getLogger(LedgerActivitiesImpl.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() { };

    private final String baseUrl;
    private final HttpClient http;

    public LedgerActivitiesImpl(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    @Override
    public HoldResult placeHold(PlaceHoldInput input) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("request_id", input == null ? null : input.requestId());
        body.put("amount", input == null ? null : input.amount());
        return call("PlaceHold", "/ledger/hold", body);
    }

    @Override
    public HoldResult commitHold(HoldRef input) {
        return call("CommitHold", "/ledger/commit", holdBody(input));
    }

    @Override
    public HoldResult releaseHold(HoldRef input) {
        return call("ReleaseHold", "/ledger/release", holdBody(input));
    }

    private static Map<String, Object> holdBody(HoldRef input) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("hold_id", input == null ? null : input.holdId());
        return body;
    }

    private HoldResult call(String activity, String path, Map<String, Object> body) {
        try {
            return doCall(activity, path, body);
        } catch (ApplicationFailure e) {
            throw e;
        } catch (RuntimeException e) {
            log.error("{} failed unexpectedly", activity, e);
            throw e;
        }
    }

    private HoldResult doCall(String activity, String path, Map<String, Object> body) {
        ActivityInfo info = Activity.getExecutionContext().getInfo();
        String subject = body.containsKey("request_id")
                ? "request_id=" + body.get("request_id")
                : "hold_id=" + body.get("hold_id");

        int status;
        Map<String, Object> response;
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                    .timeout(Duration.ofSeconds(60))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(JSON.writeValueAsBytes(body)))
                    .build();
            HttpResponse<byte[]> raw = http.send(request, HttpResponse.BodyHandlers.ofByteArray());
            status = raw.statusCode();
            response = parse(raw.body());
        } catch (IOException e) {
            log.warn("{} {} attempt={} -> ledger unreachable: {}", activity, subject, info.getAttempt(), e.toString());
            throw ApplicationFailure.newFailureWithCause(
                    "ledger unreachable at " + baseUrl + ": " + e.getMessage(), "LedgerUnavailable", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ApplicationFailure.newFailure("interrupted while calling the ledger", "LedgerUnavailable");
        }

        String message = response.get("error") instanceof String s ? s : "HTTP " + status;
        if (status >= 200 && status < 300) {
            HoldResult result = new HoldResult(str(response.get("hold_id")), str(response.get("status")));
            log.info("{} {} attempt={} -> {} {}", activity, subject, info.getAttempt(), result.holdId(), result.status());
            return result;
        }

        log.info("{} {} attempt={} -> HTTP {}: {}", activity, subject, info.getAttempt(), status, message);
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("http_status", status);
        details.put("error", message);
        details.putAll(body);
        switch (status) {
            case 422 -> throw ApplicationFailure.newNonRetryableFailure(message, "LedgerRejected", details);
            case 404 -> throw ApplicationFailure.newNonRetryableFailure(message, "LedgerNotFound", details);
            case 409 -> throw ApplicationFailure.newNonRetryableFailure(message, "LedgerConflict", details);
            default -> {
                if (status >= 500) {
                    throw ApplicationFailure.newFailure(message, "LedgerUnavailable", details);
                }
                throw ApplicationFailure.newNonRetryableFailure(message, "LedgerBadRequest", details);
            }
        }
    }

    private static Map<String, Object> parse(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return Map.of();
        }
        try {
            Map<String, Object> m = JSON.readValue(bytes, MAP);
            return m == null ? Map.of() : m;
        } catch (IOException e) {
            return Map.of();
        }
    }

    private static String str(Object value) {
        return value == null ? null : value.toString();
    }
}
