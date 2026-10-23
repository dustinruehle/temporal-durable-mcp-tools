package heritage.ledger;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LedgerTest {

    private Ledger ledger;

    @BeforeEach
    void setUp() {
        ledger = new Ledger(0);
    }

    @Test
    void holdIsIdempotentPerRequestWhileOpen() {
        Ledger.Result first = ledger.hold(Map.of("request_id", "r1", "amount", 100));
        Ledger.Result second = ledger.hold(Map.of("request_id", "r1", "amount", 100));

        assertEquals(200, first.status());
        assertEquals("H-r1-1", first.body().get("hold_id"));
        assertEquals("held", first.body().get("status"));
        assertEquals(first.body(), second.body());
        assertEquals(1, ((List<?>) ledger.holds().get("holds")).size());
        assertEquals(1L, ledger.stats().get("open_holds"));

        // Once the hold is closed, a new hold for the same request gets the next sequence number.
        ledger.release(Map.of("hold_id", "H-r1-1"));
        assertEquals("H-r1-2", ledger.hold(Map.of("request_id", "r1", "amount", 100)).body().get("hold_id"));
    }

    @Test
    void releaseAfterCommitIsConflict() {
        String holdId = (String) ledger.hold(Map.of("request_id", "r2", "amount", 5)).body().get("hold_id");

        assertEquals(200, ledger.commit(Map.of("hold_id", holdId)).status());
        assertEquals(200, ledger.commit(Map.of("hold_id", holdId)).status(), "commit is idempotent");
        assertEquals(409, ledger.release(Map.of("hold_id", holdId)).status());
        assertEquals(404, ledger.commit(Map.of("hold_id", "H-missing-1")).status());

        Map<String, Object> stats = ledger.stats();
        assertEquals(1L, stats.get("committed"));
        assertEquals(0L, stats.get("open_holds"));
    }

    @Test
    void releaseIsIdempotentAndCommitAfterReleaseIsConflict() {
        String holdId = (String) ledger.hold(Map.of("request_id", "r3", "amount", 5)).body().get("hold_id");

        assertEquals("released", ledger.release(Map.of("hold_id", holdId)).body().get("status"));
        assertEquals("released", ledger.release(Map.of("hold_id", holdId)).body().get("status"));
        assertEquals(409, ledger.commit(Map.of("hold_id", holdId)).status());
    }

    @Test
    void failNextReturns503ThenRecovers() {
        ledger.setFault(Map.of("service", "hold", "mode", "fail_next", "count", 2));

        assertEquals(503, ledger.hold(Map.of("request_id", "r4", "amount", 1)).status());
        assertEquals(503, ledger.hold(Map.of("request_id", "r4", "amount", 1)).status());
        assertEquals(200, ledger.hold(Map.of("request_id", "r4", "amount", 1)).status());

        @SuppressWarnings("unchecked")
        Map<String, Object> holdStats = (Map<String, Object>) ledger.stats().get("hold");
        assertEquals(3L, holdStats.get("calls"));
        assertEquals(1L, holdStats.get("ok"));
        assertEquals(2L, holdStats.get("errors"));
        assertEquals(0L, holdStats.get("rejected_429"));
        assertEquals(0L, holdStats.get("in_flight"));
        assertEquals(1L, ledger.stats().get("open_holds"), "failed calls create no holds");
    }

    @Test
    void rejectNextReturns422WithoutChangingState() {
        String holdId = (String) ledger.hold(Map.of("request_id", "r5", "amount", 1)).body().get("hold_id");
        ledger.setFault(Map.of("service", "commit", "mode", "reject_next", "count", 1));

        assertEquals(422, ledger.commit(Map.of("hold_id", holdId)).status());
        assertEquals(1L, ledger.stats().get("open_holds"));
        assertEquals(200, ledger.release(Map.of("hold_id", holdId)).status());
    }

    @Test
    void resetClearsHoldsFaultsAndStats() {
        ledger.hold(Map.of("request_id", "r6", "amount", 1));
        ledger.setFault(Map.of("service", "release", "mode", "slow", "delay_ms", 500));
        ledger.reset();

        assertEquals(0, ((List<?>) ledger.holds().get("holds")).size());
        @SuppressWarnings("unchecked")
        Map<String, Object> releaseFaults = (Map<String, Object>) ledger.faults().get("release");
        assertEquals(0, releaseFaults.get("delay_ms"));
        @SuppressWarnings("unchecked")
        Map<String, Object> holdStats = (Map<String, Object>) ledger.stats().get("hold");
        assertEquals(0L, holdStats.get("calls"));
        assertEquals("H-r6-1", ledger.hold(Map.of("request_id", "r6", "amount", 1)).body().get("hold_id"));
    }
}
