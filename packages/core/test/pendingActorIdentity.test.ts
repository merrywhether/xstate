import { describe, expect, it } from 'vitest';
import {
  createMachine,
  setup,
  stopActor,
  type AnyActor
} from '../src/index.ts';
import { createDurable } from '../src/durable/index.ts';
import { deliverEvent } from '../src/runtimeHelpers.ts';
import { restoreActor } from '../src/persist/index.ts';

describe('pending effect actor identity', () => {
  it('does not persist a stop for an old child as a stop for its replacement', async () => {
    const child = createMachine({
      context: { count: 0 },
      on: { INC: ({ context }) => ({ context: { count: context.count + 1 } }) }
    });
    const machine = createMachine({
      actors: { child },
      initial: 'active',
      states: {
        active: {
          invoke: { id: 'worker', src: 'child' },
          on: { RESET: { target: 'active', reenter: true } }
        }
      }
    });
    const durable = createDurable(machine, {
      executeAction: (effect) => effect.exec(),
      waitForEvent: () => ({ type: 'RESET' }),
      spawnActor: () => {},
      startActor: (actor) => {
        actor.start();
      },
      stopActor,
      sendEvent: (source, target, event) => deliverEvent(source, target, event)
    });
    let [snapshot, effects] = durable.initialTransition();
    await durable.executeEffects(effects);
    const old = snapshot.children.worker!;
    [snapshot, effects] = durable.transition(snapshot, { type: 'RESET' });
    expect(snapshot.children.worker).not.toBe(old);
    expect(snapshot.children.worker!.address).toBe(old.address);
    expect(() => machine.getPersistedSnapshot(snapshot)).toThrow(
      /actor is no longer addressable/
    );
    await durable.executeEffects(effects);
    snapshot.children.worker!.send({ type: 'INC' });
    expect(snapshot.children.worker!.getSnapshot().context.count).toBe(1);
    const saved = JSON.parse(
      JSON.stringify(machine.getPersistedSnapshot(snapshot))
    );
    const restored = restoreActor(machine, saved).start();
    restored.getSnapshot().children.worker!.send({ type: 'INC' });
    expect(
      restored.getSnapshot().children.worker!.getSnapshot().context.count
    ).toBe(2);
    restored.stop();
  });

  it.each(['send', 'action'] as const)(
    'rejects a pending %s that refers to a displaced child incarnation',
    async (kind) => {
      const child = createMachine({ on: { INC: {} } });
      let old: AnyActor | undefined;
      const machine = setup({
        actors: { child },
        actions: { record: (_args: { ref: AnyActor }) => {} }
      }).createMachine({
        context: { captured: false },
        initial: 'active',
        states: {
          active: {
            invoke: { src: 'child', id: 'worker' },
            on: {
              REPLACE: { target: 'active', reenter: true },
              CAPTURE: (args, enq) => {
                if (kind === 'send') enq.sendTo(old!, { type: 'INC' });
                else enq(args.actions.record, { ref: old! });
                return { context: { captured: true } };
              }
            }
          }
        }
      });
      const durable = createDurable(machine, {
        executeAction: (effect) => effect.exec(),
        waitForEvent: () => ({ type: 'CAPTURE' }),
        spawnActor: () => {},
        startActor: (actor) => {
          actor.start();
        },
        stopActor,
        sendEvent: (source, target, event) =>
          deliverEvent(source, target, event)
      });
      let [snapshot, effects] = durable.initialTransition();
      await durable.executeEffects(effects);
      old = snapshot.children.worker!;
      [snapshot, effects] = durable.transition(snapshot, { type: 'REPLACE' });
      await durable.executeEffects(effects);
      expect(snapshot.children.worker!.sessionId).not.toBe(old.sessionId);
      [snapshot] = durable.transition(snapshot, { type: 'CAPTURE' });
      expect(() => machine.getPersistedSnapshot(snapshot)).toThrow(
        /actor is no longer addressable/
      );
    }
  );
});
