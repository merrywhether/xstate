import { describe, expect, it } from 'vitest';
import { createActor, setup } from '../src/index.ts';
import { restoreActor } from '../src/persist/index.ts';

describe('failed effect recovery', () => {
  it('does not restore a failed action or its unstarted tail', () => {
    let failures = 0;
    let tails = 0;
    let saved: unknown;
    const machine = setup({
      actions: {
        fail: () => {
          failures++;
          if (failures === 1) throw new Error('failure');
        },
        tail: () => {
          tails++;
        }
      }
    }).createMachine({
      initial: 'active',
      states: {
        active: {
          on: {
            GO: ({ actions }, enq) => {
              enq(actions.fail);
              enq(actions.tail);
            }
          },
          onError: { target: 'recovered' }
        },
        recovered: {}
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
    expect(restored.getSnapshot().matches('recovered')).toBe(true);
    expect(failures).toBe(1);
    expect(tails).toBe(0);
  });

  it('routes a restored initial action failure through the owning actor error handler', () => {
    let failures = 0;
    let tails = 0;
    const machine = setup({
      actions: {
        fail: () => {
          failures++;
          throw new Error('failure');
        },
        tail: () => {
          tails++;
        }
      }
    }).createMachine({
      initial: 'active',
      states: {
        active: {
          entry: ({ actions }, enq) => {
            enq(actions.fail);
            enq(actions.tail);
          },
          onError: { target: 'recovered' }
        },
        recovered: {}
      }
    });
    const original = createActor(machine);
    const saved = JSON.parse(JSON.stringify(original.getPersistedSnapshot()));
    const restored = restoreActor(machine, saved).start();
    expect(restored.getSnapshot().matches('recovered')).toBe(true);
    expect(failures).toBe(1);
    expect(tails).toBe(0);
  });
  it('does not retry a custom action after it returns a pending promise', () => {
    let calls = 0;
    const wait: () => void = () => {
      calls++;
      return new Promise<void>(() => {});
    };
    const machine = setup({
      actions: { wait }
    }).createMachine({ on: { GO: ({ actions }, enq) => enq(actions.wait) } });
    const actor = createActor(machine).start();
    actor.send({ type: 'GO' });
    const saved = JSON.parse(JSON.stringify(actor.getPersistedSnapshot()));
    restoreActor(machine, saved).start();
    expect(calls).toBe(1);
  });
});
