import { describe, expect, it } from 'vitest';
import { createActor, createMachine, setup } from '../src/index.ts';
import { restoreActor } from '../src/persist/index.ts';

describe('actor mailbox persistence', () => {
  it('restores the current child input captured before transition commit', () => {
    let capture: (() => void) | undefined;
    let saved: unknown;
    const child = createMachine({
      context: { count: 0 },
      on: {
        INC: ({ context }) => {
          capture?.();
          return { context: { count: context.count + 1 } };
        }
      }
    });
    const machine = setup({ actors: { child } }).createMachine({
      invoke: { id: 'child', src: 'child' },
      on: {
        GO: ({ children }, enq) => enq.sendTo(children.child!, { type: 'INC' })
      }
    });
    const actor = createActor(machine).start();
    capture = () => {
      capture = undefined;
      saved = JSON.parse(JSON.stringify(actor.getPersistedSnapshot()));
    };
    actor.send({ type: 'GO' });
    const restored = restoreActor(
      machine,
      saved as ReturnType<typeof actor.getPersistedSnapshot>
    ).start();
    expect(
      restored.getSnapshot().children.child!.getSnapshot().context.count
    ).toBe(1);
  });

  it('restores events queued before the actor starts', () => {
    const machine = createMachine({
      context: { count: 0 },
      on: { INC: ({ context }) => ({ context: { count: context.count + 1 } }) }
    });
    const actor = createActor(machine);
    actor.send({ type: 'INC' });
    const saved = JSON.parse(JSON.stringify(actor.getPersistedSnapshot()));
    const restored = restoreActor(machine, saved).start();
    expect(restored.getSnapshot().context.count).toBe(1);
  });
});

describe('mailbox recovery across actor incarnations', () => {
  it.each([false, true])(
    'only rebases the current child completion (stale: %s)',
    (stale) => {
      const child = createMachine({});
      let queueAndCapture: (() => void) | undefined;
      let saved: unknown;
      const machine = setup({ actors: { child } }).createMachine({
        id: 'parent',
        initial: 'working',
        states: {
          working: {
            invoke: { id: 'w', src: 'child', onDone: { target: 'finished' } },
            on: {
              REPLACE: { target: 'working', reenter: true },
              CAPTURE: () => queueAndCapture?.()
            }
          },
          finished: { type: 'final' }
        }
      });
      const actor = createActor(machine).start();
      const previous = actor.getSnapshot().children.w!;
      actor.send({ type: 'REPLACE' });
      const current = actor.getSnapshot().children.w!;
      expect(current.sessionId).not.toBe(previous.sessionId);
      queueAndCapture = () => {
        queueAndCapture = undefined;
        const sender = stale ? previous : current;
        actor._send(
          {
            type: 'xstate.done.actor',
            actorId: 'w',
            output: undefined,
            sessionId: sender.sessionId
          } as never,
          sender
        );
        saved = JSON.parse(JSON.stringify(actor.getPersistedSnapshot()));
      };
      actor.send({ type: 'CAPTURE' });
      const restored = restoreActor(
        machine,
        saved as ReturnType<typeof actor.getPersistedSnapshot>
      ).start();
      expect(restored.getSnapshot().value).toBe(stale ? 'working' : 'finished');
    }
  );

  it('does not replay the input after an error recovery snapshot commits', () => {
    let saved: unknown;
    const machine = createMachine({
      context: { replayed: 0 },
      initial: 'active',
      states: {
        active: {
          on: {
            GO: () => {
              throw new Error('failure');
            }
          },
          onError: { target: 'recovered' }
        },
        recovered: {
          on: {
            GO: ({ context }) => ({
              context: { replayed: context.replayed + 1 }
            })
          }
        }
      }
    });
    const actor = createActor(machine).start();
    actor.subscribe((snapshot) => {
      if (snapshot.matches('recovered'))
        saved = JSON.parse(JSON.stringify(actor.getPersistedSnapshot()));
    });
    actor.send({ type: 'GO' });
    const restored = restoreActor(
      machine,
      saved as ReturnType<typeof actor.getPersistedSnapshot>
    ).start();
    expect(restored.getSnapshot().context.replayed).toBe(0);
  });
});
