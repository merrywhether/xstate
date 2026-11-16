import { describe, expect, it } from 'vitest';
import {
  createActor,
  createMachine,
  SimulatedClock,
  types
} from '../src/index.ts';

function replaceTimer(replacementId: string, fireWhileQueued: boolean) {
  const clock = new SimulatedClock();
  let replace: () => void;
  const machine = createMachine({
    schemas: {
      context: types<{ expired: string[] }>(),
      events: {
        START: types<{ id: string; label: string }>(),
        CANCEL: types<{}>(),
        REPLACE: types<{}>()
      },
      internalEvents: { EXPIRE: types<{ label: string }>() }
    },
    context: { expired: [] },
    on: {
      START: ({ event }, enq) => {
        enq.raise(
          { type: 'EXPIRE', label: event.label },
          { id: event.id, delay: 100 }
        );
      },
      CANCEL: (_, enq) => enq.cancel('shared'),
      EXPIRE: ({ context, event }) => ({
        context: { expired: [...context.expired, event.label] }
      }),
      REPLACE: (_, enq) => enq(() => replace())
    }
  });
  const actor = createActor(machine, { clock }).start();
  replace = () => {
    actor.send({ type: 'CANCEL' });
    actor.send({ type: 'START', id: replacementId, label: 'replacement' });
    // The actor is busy with this action. The native timer callback's
    // notification queues behind CANCEL and START.
    if (fireWhileQueued) clock.increment(100);
  };

  try {
    actor.send({ type: 'START', id: 'shared', label: 'original' });
    actor.send({ type: 'REPLACE' });
    const afterReplacement = actor.getSnapshot().context.expired;
    clock.increment(99);
    const beforeDeadline = actor.getSnapshot().context.expired;
    clock.increment(1);
    return {
      afterReplacement,
      beforeDeadline,
      atDeadline: actor.getSnapshot().context.expired
    };
  } finally {
    actor.stop();
  }
}

describe('replacement of a named timer', () => {
  const expected = {
    afterReplacement: [],
    beforeDeadline: [],
    atDeadline: ['replacement']
  };

  it('permits reuse when cancellation precedes the timer callback', () => {
    expect(replaceTimer('shared', false)).toEqual(expected);
  });

  it('ignores a queued firing when the replacement has a different ID', () => {
    expect(replaceTimer('different', true)).toEqual(expected);
  });

  it.fails('ignores a queued firing when the replacement reuses the ID', () => {
    expect(replaceTimer('shared', true)).toEqual(expected);
  });
});
