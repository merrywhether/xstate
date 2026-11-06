import { Effect } from 'effect';
import { checkpoint, createEffectActor } from '@xstate/effect';
import { createMachine, type Snapshot } from 'xstate';

const review = createMachine({
  id: 'review',
  initial: 'draft',
  states: {
    draft: { on: { SUBMIT: { target: 'inReview' } } },
    inReview: { on: { APPROVE: { target: 'approved' } } },
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
      actor.send(event);
      return JSON.parse(
        yield* checkpoint(actor, JSON.stringify)
      ) as Snapshot<unknown>;
    })
  );

const program = Effect.gen(function* () {
  const submitted = yield* handle(undefined, { type: 'SUBMIT' });
  // Store `submitted` in a database row or a Durable Object, possibly for days.
  const approved = yield* handle(submitted, { type: 'APPROVE' });
  return approved.status;
});

export const result = await Effect.runPromise(program);
console.log(result); // 'done'
