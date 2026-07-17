# Act 1 design note: audit before abstraction

M0 sends one trusted in-process event to one preconfigured destination. The
Promise and Effect workflows share the same pure status policy and the same
injected HTTP adapter.

| Observation | Source of the pressure | M0 decision |
|---|---|---|
| A `503` is a completed delivery attempt, not a transport rejection. | Relay's domain policy. | Preserve it as `Rejected` data in both workflows. |
| Calling the async Promise function starts the client immediately. | Promise execution semantics. | Accept this in the baseline; the Effect workflow is a lazy description. |
| A Promise may reject with any JavaScript value, although `Promise<A>` exposes no rejection type. | Promise and TypeScript interoperation. | Map the adapter rejection to `DeliveryTransportError` at the Effect boundary. |
| Cancellation needs an owner and the client must observe its signal. | Cooperative cancellation plus application ownership. | The Promise caller owns an `AbortController`; Effect owns the adapter signal and accepts a host signal at its run boundary. |
| A response body must be discarded after reading the status. | The HTTP adapter's resource contract. | Keep `try` / `finally` inside the native adapter in both workflows. |
| Global `fetch`, configuration, and test timing can become hidden dependencies. | Missing application architecture. | Inject the client now; wait for real dependency pressure before adding Effect services and Layers. |

Effect does not choose the HTTP success policy, redact secrets automatically,
guarantee that a remote server stopped after cancellation, or make a webhook
exactly once. M0 also has no validation, retry, persistence, concurrency, or
crash recovery. Those requirements arrive in later checkpoints.
