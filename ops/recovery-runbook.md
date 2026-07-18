# Relay dead-letter recovery runbook

Use this runbook after a delivery reaches `DeadLettered`. The objective is not
to make the alert disappear. It is to choose a deliberate next state without
losing the accepted route, attempt history, or evidence needed to explain what
happened.

## Before changing state

1. Stabilize the receiver or the source of pressure. Replaying into the same
   outage can turn one poison delivery into a retry storm.
2. Inspect admission rejections, active attempts, retry delay, dead-letter
   growth, reconciliation failures, and fencing rejections in
   `relay-m9-dashboard.json`.
3. Inspect the delivery and its ordered attempt history:

   ```bash
   curl --fail-with-body \
     --header "Authorization: Bearer ${RELAY_OPERATIONS_TOKEN}" \
     "${RELAY_URL}/operations/deliveries/${DELIVERY_ID}"
   ```

4. Confirm that the delivery is still `DeadLettered`. Every mutation is
   conditional on that state. A `409 not_dead_lettered` response means another
   action won the race; inspect again instead of forcing an update.

The operations credential is configured independently from the intake
credential with `RELAY_OPERATIONS_AUTHORIZATION`. Possession of that bearer
token authorizes the action; it does not identify a human operator or create a
durable audit record. Record the incident and change in the organization’s
audit system when actor identity or approval history matters.

### Optional delivery analysis

An application host may call Relay's read-only delivery analyst before an
operator chooses an action. The model receives only the delivery ID,
destination ID, current state, total attempt count, and the latest 20 attempt
classifications. It does not receive event payloads, endpoints, credentials,
claim ownership, or trace IDs.

Treat its summary and two proposed checks as navigation, not evidence or
permission. Verify every claim against `deliveryStatus` and the receiver. The
analyst cannot retry, repair, terminate, or reconcile work. If the model is
missing, fails, or returns invalid structured output, the host receives a
deterministic fallback that points back to the ordered history and this
runbook. Both paths append the returned report to Relay's bounded process-local
analysis history.

## Choose one action

| Action | Use when | State change | Route used next |
|---|---|---|---|
| Retry | The snapshotted endpoint is still the intended destination and replay is safe enough. | `DeadLettered → Pending` | The route retained when the event was accepted. |
| Repair | The same destination has a corrected trusted endpoint or configuration version. | `DeadLettered → Pending` | Relay’s current configured route. A different destination ID is rejected. |
| Terminate | Policy says this work must never be attempted again. | `DeadLettered → Terminated` | None. The attempt history remains inspectable. |

Retry and repair preserve the delivery ID and prior attempt ordinals. They do
not erase remote ambiguity: if an earlier request reached the receiver before
Relay lost the response, replay can repeat the remote effect. Confirm that the
receiver enforces Relay’s stable idempotency key, or explicitly accept that
risk.

Apply exactly one action:

```bash
curl --fail-with-body \
  --request POST \
  --header "Authorization: Bearer ${RELAY_OPERATIONS_TOKEN}" \
  "${RELAY_URL}/operations/dead-letters/${DELIVERY_ID}/retry"
```

Replace the last path segment with `repair` or `terminate` for the other
decisions. If the HTTP response is lost, inspect the delivery before repeating
the request. The committed state, not the missing response, is authoritative.

## Discover eligible work now

The normal reconciler polls automatically. After a retry or repair, an operator
may request one bounded pass rather than waiting for the next interval:

```bash
curl --fail-with-body \
  --request POST \
  --header "Authorization: Bearer ${RELAY_OPERATIONS_TOKEN}" \
  "${RELAY_URL}/operations/reconcile"
```

The response reports how many eligible deliveries were claimed. The configured
claim batch size bounds the pass. Claim leases and generations still fence
stale owners; this endpoint does not bypass the repository’s ownership rules.

## Verify the outcome

1. Inspect the delivery again. A replay should append a new attempt ordinal;
   termination should preserve the existing history unchanged.
2. Check `relay_delivery_dead_letter_actions_total` for the selected action and
   find the `delivery.dead_letter.action_applied` log by delivery ID.
3. Check reconciliation pass failures, claim lag, fencing rejections, and new
   dead-letter growth. Stop replaying if the original failure pattern returns.
4. Close the incident only after the receiver’s externally visible state has
   also been checked. Relay’s local terminal state cannot prove what a remote
   service committed.

## Limits of this fixture

- Dead-letter listing returns the first 50 records and has no pagination or
  tenant filter.
- Actions are one delivery at a time; there is no bulk replay control.
- The bearer-token fixture has no actor identity, approval workflow, or durable
  administrative audit log.
- The optional analyst history is process-local, keeps only the latest 100
  reports, and is lost on restart. Export it to a durable actor-aware audit
  system before relying on it for compliance or approval evidence.
- Metrics are process-local until an exporter and fleet-wide aggregation are
  configured. The dashboard queries assume Prometheus-compatible metric names.
- Repair can replace an endpoint only for the delivery’s existing destination
  ID. Moving work to a different destination is a new domain operation, not a
  repair.
- Neither reconciliation nor idempotency keys provide exactly-once remote
  effects.
