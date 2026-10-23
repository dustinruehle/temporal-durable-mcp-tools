package heritage.ledger;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Ledger Activities on the {@code heritage-ledger} Task Queue.
 *
 * <p>The Activity type names are set explicitly so Workflows in other SDKs can call them by name.
 * Each Activity takes one JSON object and returns one JSON object, with snake_case fields.
 */
@ActivityInterface
public interface LedgerActivities {

    String TASK_QUEUE = "heritage-ledger";

    /** Input {"request_id", "amount"}; output {"hold_id", "status": "held"}. */
    @ActivityMethod(name = "PlaceHold")
    HoldResult placeHold(PlaceHoldInput input);

    /** Input {"hold_id"}; output {"hold_id", "status": "committed"}. */
    @ActivityMethod(name = "CommitHold")
    HoldResult commitHold(HoldRef input);

    /** Input {"hold_id"}; output {"hold_id", "status": "released"}. */
    @ActivityMethod(name = "ReleaseHold")
    HoldResult releaseHold(HoldRef input);

    @JsonIgnoreProperties(ignoreUnknown = true)
    record PlaceHoldInput(
            @JsonProperty("request_id") String requestId,
            @JsonProperty("amount") Number amount) { }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record HoldRef(@JsonProperty("hold_id") String holdId) { }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record HoldResult(
            @JsonProperty("hold_id") String holdId,
            @JsonProperty("status") String status) { }
}
