import { describe, expect, it, vi } from 'vitest';
import { createMachine, type AnyActor } from '../src/index.ts';
import { createDurable } from '../src/durable/index.ts';

describe('pending remote sends', () => {
  it('restores a send to an address outside the embedded tree', async () => {
    const target = { $actor: 'outside/worker', incarnation: 'generation-1' };
    const machine = createMachine({
      on: {
        GO: (_, enq) =>
          enq.sendTo(target as unknown as AnyActor, { type: 'PING' })
      }
    });
    const adapter = {
      executeAction: vi.fn(),
      enqueueRootEvent: vi.fn(),
      waitForEvent: vi.fn(() => ({ type: 'GO' }))
    };
    const fresh = createDurable(machine, adapter);
    const [initial] = fresh.initialTransition();
    const [next] = fresh.transition(initial, { type: 'GO' });
    const persisted = JSON.parse(
      JSON.stringify(
        machine.getPersistedSnapshot(next, { embedChildren: false })
      )
    );
    const sendEvent = vi.fn();
    const resumed = createDurable(machine, { ...adapter, sendEvent });
    const [, effects] = resumed.restore(persisted);
    await resumed.executeEffects(effects);
    expect(sendEvent).toHaveBeenCalledOnce();
    expect(sendEvent.mock.calls[0][1]).toEqual(target);
    expect(sendEvent.mock.calls[0][2]).toEqual({ type: 'PING' });
  });
});
