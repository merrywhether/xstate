import { describe, expect, it } from 'vitest';
import { createActor, createMachine, deliverEvent } from '../src/index.ts';
import { createDurable } from '../src/durable/index.ts';

const copy = <T>(value: T): T => JSON.parse(JSON.stringify(value));

describe('durable delivery acceptance', () => {
  it.each([false, true])(
    'does not confuse a repeated event with delivery from another source (%s)',
    async (otherSource) => {
      const child = createMachine({
        context: { count: 0 },
        on: {
          INC: ({ context }) => ({ context: { count: context.count + 1 } })
        }
      });
      const repeated = { type: 'INC' };
      const machine = createMachine({
        actors: { child },
        invoke: { src: 'child', id: 'child' },
        on: {
          GO: ({ children }, enq) => {
            enq.sendTo(children.child!, repeated);
            enq.sendTo(children.child!, repeated);
          }
        }
      });
      const other = createActor(createMachine({})).start();
      let current: ReturnType<typeof machine.getInitialSnapshot>;
      let checkpoint:
        | ReturnType<typeof machine.getPersistedSnapshot>
        | undefined;
      let sends = 0;
      const execution = createDurable(machine, {
        executeAction: (effect) => effect.exec(),
        waitForEvent: () => ({ type: 'GO' }),
        spawnActor: () => {},
        startActor: (actor) => {
          actor.start();
        },
        sendEvent: (source, target, event) => {
          if (++sends === 1) {
            if (otherSource) deliverEvent(other, target, event);
            else deliverEvent(source, target, event);
            checkpoint = copy(machine.getPersistedSnapshot(current));
            if (!otherSource) return;
          }
          deliverEvent(source, target, event);
        }
      });
      const [initial, initialEffects] = execution.initialTransition();
      await execution.executeEffects(initialEffects);
      const [next, effects] = execution.transition(initial, { type: 'GO' });
      current = next;
      await execution.executeEffects(effects);
      expect(checkpoint).toBeDefined();
      if (!checkpoint) throw new Error('Checkpoint was not captured.');
      const resumed = createDurable(machine, {
        executeAction: (effect) => effect.exec(),
        waitForEvent: () => ({ type: 'GO' }),
        spawnActor: () => {},
        startActor: (actor) => {
          actor.start();
        },
        sendEvent: (source, target, event) =>
          deliverEvent(source, target, event)
      });
      const [restored, restoredEffects] = resumed.restore(checkpoint);
      await resumed.executeEffects(restoredEffects);
      expect(next.children.child!.getSnapshot().context.count).toBe(
        otherSource ? 3 : 2
      );
      expect(restored.children.child!.getSnapshot().context.count).toBe(
        next.children.child!.getSnapshot().context.count
      );
      other.stop();
    }
  );
});
