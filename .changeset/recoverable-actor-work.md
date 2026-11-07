---
"xstate": patch
---

Preserve queued actor inputs and unfinished transition effects through the existing persisted snapshot and restore APIs, including pre-start checkpoints, child completion, and partially executed effect batches. Keep restored input ahead of newer sends and report invalid recovery data through the actor error lifecycle.

Pending actions require registered implementations and supported captured values. Captured live actor capabilities must still refer to the same incarnation; unavailable retired actors reject capture until their pending operations are accepted. Running asynchronous activities retain their restart semantics, and external operations still need idempotency or durable journaling.

Recovery metadata requires an updated reader. Restore a pending mailbox with the runtime that captured it; other runtimes reject it instead of dropping work. Older snapshots remain readable but cannot recover work their writer omitted.
