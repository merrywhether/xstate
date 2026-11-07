import { describe, expect, it } from 'vitest';
import { createMachine, setup, type AnyActor } from '../src/index.ts';
import { deliverEvent } from '../src/runtimeHelpers.ts';
import { createDurable } from '../src/durable/index.ts';

const copy = <T>(value: T): T => JSON.parse(JSON.stringify(value));

describe('checkpoint during durable delivery', () => {
  for (const capture of ['before delivery', 'after async delivery'] as const) {
    it(`continues the pending batch from a checkpoint ${capture}`, async () => {
      const child = createMachine({
        context: { count: 0 },
        on: {
          INC: ({ context }) => ({ context: { count: context.count + 1 } })
        }
      });
      const machine = setup({ actors: { child } }).createMachine({
        invoke: { id: 'child', src: 'child' },
        on: {
          GO: ({ children }, enq) => {
            enq.sendTo(children.child!, { type: 'INC' });
            enq.sendTo(children.child!, { type: 'INC' });
          }
        }
      });
      let checkpoint:
        | ReturnType<typeof machine.getPersistedSnapshot>
        | undefined;
      let current: ReturnType<typeof machine.getInitialSnapshot>;
      let sending = 0;
      const execution = createDurable(machine, {
        executeAction: (effect) => effect.exec(),
        waitForEvent: () => ({ type: 'GO' as const }),
        spawnActor: () => {},
        startActor: (actor: AnyActor) => {
          actor.start();
        },
        sendEvent: async (source, target, event) => {
          sending++;
          if (capture === 'before delivery' && sending === 1) {
            checkpoint = copy(machine.getPersistedSnapshot(current));
          }
          if (capture === 'after async delivery') await Promise.resolve();
          deliverEvent(source, target, event);
        }
      });
      const [initial, initialEffects] = execution.initialTransition();
      await execution.executeEffects(initialEffects);
      if (capture === 'after async delivery') {
        initial.children.child!.subscribe((snapshot) => {
          if (snapshot.context.count === 1 && !checkpoint) {
            checkpoint = copy(machine.getPersistedSnapshot(current));
          }
        });
      }
      const [next, effects] = execution.transition(initial, { type: 'GO' });
      current = next;
      await execution.executeEffects(effects);
      expect(next.children.child!.getSnapshot().context.count).toBe(2);
      expect(checkpoint).toBeDefined();
      const resumed = createDurable(machine, {
        executeAction: (effect) => effect.exec(),
        waitForEvent: () => ({ type: 'GO' as const }),
        spawnActor: () => {},
        startActor: (actor: AnyActor) => {
          actor.start();
        },
        sendEvent: (source, target, event) =>
          deliverEvent(source, target, event)
      });
      const [restored, restoredEffects] = resumed.restore(checkpoint!);
      await resumed.executeEffects(restoredEffects);
      expect(restored.children.child!.getSnapshot().context.count).toBe(2);
    });
  }
});
