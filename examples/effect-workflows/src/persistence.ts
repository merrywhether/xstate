import { Effect } from 'effect';
import { createEffectActor, waitFor } from '@xstate/effect';
import { sendAndAwait } from '@xstate/effect/test';
import { setup, type Snapshot } from 'xstate';
import { createAsyncLogic } from 'xstate/actors';

const review = setup({
  actors: {
    finalizeReview: createAsyncLogic({ run: () => Promise.resolve() })
  }
}).createMachine({
  id: 'review',
  initial: 'draft',
  states: {
    draft: { on: { SUBMIT: { target: 'inReview' } } },
    inReview: { on: { APPROVE: { target: 'finalizing' } } },
    finalizing: {
      invoke: { src: 'finalizeReview', onDone: { target: 'approved' } }
    },
    approved: { type: 'final' }
  }
});

// Handle one event, then return the snapshot to store.
const handle = (
  stored: Snapshot<unknown> | undefined,
  event: { type: 'SUBMIT' } | { type: 'APPROVE' }
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const actor = yield* createEffectActor(review, { snapshot: stored });
      yield* sendAndAwait(actor, event);
      return JSON.parse(JSON.stringify(actor.getPersistedSnapshot()));
    })
  );

const program = Effect.gen(function* () {
  const submitted = yield* handle(undefined, { type: 'SUBMIT' });
  // Store `submitted` in a database row or a Durable Object, possibly for days.
  const finalizing = yield* handle(submitted, { type: 'APPROVE' });
  // Resume pending work without resending APPROVE.
  const restored = yield* createEffectActor(review, { snapshot: finalizing });
  const approved = yield* waitFor(
    restored,
    (snapshot) => snapshot.status === 'done'
  );
  return approved.status;
});

export const result = await Effect.runPromise(Effect.scoped(program));
console.log(result); // 'done'
