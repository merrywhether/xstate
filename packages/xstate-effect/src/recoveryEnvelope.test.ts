import { Effect } from 'effect';
import { expect, it } from 'vitest';
import { createActor, createMachine, type AnyActorLogic } from 'xstate';
import { restoreActor } from 'xstate/persist';
import { createEffectActor } from './index.ts';

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

it('preserves opaque snapshot fields in the Effect interpreter', async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(logic);
        const saved = JSON.parse(JSON.stringify(actor.getPersistedSnapshot()));
        const restored = yield* createEffectActor(logic, { snapshot: saved });
        expect(restored.getSnapshot()).toEqual(saved);
      })
    )
  );
});

it('rejects overwriting an opaque envelope property for a queued Effect event', async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(logic);
        actor.send({ type: 'QUEUED' });
        expect(() => actor.getPersistedSnapshot()).toThrow(
          '_xstateRecovery is already in use'
        );
      })
    )
  );
});

it('rejects a native pending mailbox instead of losing it', async () => {
  const machine = createMachine({
    context: { count: 0 },
    on: { INC: ({ context }) => ({ context: { count: context.count + 1 } }) }
  });
  const native = createActor(machine);
  native.send({ type: 'INC' });
  const saved = JSON.parse(JSON.stringify(native.getPersistedSnapshot()));
  await expect(
    Effect.runPromise(
      Effect.scoped(createEffectActor(machine, { snapshot: saved }))
    )
  ).rejects.toThrow('another runtime');
});

it('rejects an Effect pending mailbox in a native actor', async () => {
  const machine = createMachine({
    context: { count: 0 },
    on: { INC: ({ context }) => ({ context: { count: context.count + 1 } }) }
  });
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(machine);
        actor.send({ type: 'INC' });
        const saved = JSON.parse(JSON.stringify(actor.getPersistedSnapshot()));
        const restored = restoreActor(machine, saved);
        expect(restored.getSnapshot().status).toBe('error');
        expect(String(restored.getSnapshot().error)).toContain(
          'another runtime'
        );
      })
    )
  );
});

it('rejects a durable pending mailbox in an Effect actor', async () => {
  const machine = createMachine({});
  const saved = {
    ...createActor(machine).getPersistedSnapshot(),
    _xstateRecovery: {
      type: 'xstate.recovery',
      version: 1,
      durableMailbox: [{}]
    }
  };
  await expect(
    Effect.runPromise(
      Effect.scoped(createEffectActor(machine, { snapshot: saved }))
    )
  ).rejects.toThrow('another runtime');
});

it('rejects malformed Effect session metadata before removing it', async () => {
  const machine = createMachine({});
  const saved = {
    ...createActor(machine).getPersistedSnapshot(),
    _xstateRecovery: {
      type: 'xstate.recovery',
      version: 1,
      effectSessions: { child: 42 }
    }
  };
  await expect(
    Effect.runPromise(
      Effect.scoped(createEffectActor(machine, { snapshot: saved }))
    )
  ).rejects.toThrow('Invalid recovery metadata');
});
