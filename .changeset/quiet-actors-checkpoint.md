---
"@xstate/effect": minor
---

Add `checkpoint(actor, encode)` to capture persisted state after queued events and their immediate effects, including child replies. Unlike an immediate `getPersistedSnapshot()` read, the checkpoint includes a completed child's pending parent notification. The encoder must synchronously return a string. Running work and future timers are not awaited.

```ts
actor.send({ type: 'GO' });
const json = yield* checkpoint(actor, JSON.stringify);
```
