import { describe, expect, it } from 'vitest';
import { createActor, type AnyActorLogic } from '../src/index.ts';
import { restoreActor } from '../src/persist/index.ts';

const roundTrip = <T>(value: T): T => JSON.parse(JSON.stringify(value));
const logic = {
  initialTransition: () => [
    {
      status: 'active',
      _pendingMailbox: 'mail',
      _pendingEffects: 'effects',
      _effectMailbox: 'host mail',
      _effectSessions: 'sessions',
      _xstateRecovery: { type: 'business', version: 9 }
    },
    []
  ],
  transition: (snapshot: unknown) => [snapshot, []],
  getPersistedSnapshot: (snapshot: unknown) => snapshot,
  restoreSnapshot: (snapshot: unknown) => snapshot
} as unknown as AnyActorLogic;

describe('recovery metadata envelope', () => {
  it('preserves old opaque snapshot fields without pending work', () => {
    const original = createActor(logic);
    const saved = roundTrip(original.getPersistedSnapshot());
    expect(restoreActor(logic, saved).getSnapshot()).toEqual(saved);
  });

  it('rejects overwriting an opaque envelope property with pending work', () => {
    const actor = createActor(logic);
    actor.send({ type: 'QUEUED' });
    expect(() => actor.getPersistedSnapshot()).toThrow(
      '_xstateRecovery is already in use'
    );
  });

  it.each([
    { type: 'xstate.recovery', version: 2 },
    { type: 'xstate.recovery', version: 1, mailbox: 'invalid' },
    { type: 'xstate.recovery', version: 1, effects: 'invalid' },
    { type: 'xstate.recovery', version: 1, effectMailbox: 'invalid' }
  ])('rejects malformed recognized metadata %j', (recovery) => {
    const snapshot = {
      status: 'active' as const,
      output: undefined,
      error: undefined,
      _xstateRecovery: recovery
    };
    expect(restoreActor(logic, snapshot).getSnapshot().status).toBe('error');
  });
});

it('rejects a durable mailbox in a native actor', () => {
  const snapshot = {
    status: 'active' as const,
    output: undefined,
    error: undefined,
    _xstateRecovery: {
      type: 'xstate.recovery',
      version: 1,
      durableMailbox: [{}]
    }
  };
  const restored = restoreActor(logic, snapshot);
  expect(restored.getSnapshot().status).toBe('error');
  expect(String(restored.getSnapshot().error)).toContain('another runtime');
});
