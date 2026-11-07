import { Effect } from 'effect';
import { describe, expect, it } from 'vitest';
import { createActor, createMachine, setup, types } from 'xstate';
import { restoreActor } from 'xstate/persist';
import { createEffectActor, waitFor } from './index.ts';
import { sendAndAwait } from './test.ts';

const copy = <T>(value: T): T => JSON.parse(JSON.stringify(value));
const child = setup({
  schemas: { events: { APPEND: types<{ value: string }>() } }
}).createMachine({
  context: { value: '' },
  on: {
    APPEND: ({ context, event }) => ({
      context: { value: context.value + event.value }
    })
  }
});
const parent = setup({ actors: { child } }).createMachine({
  invoke: { id: 'child', src: 'child' },
  on: { PING: {} }
});

describe('restored native children in the Effect interpreter', () => {
  it('preserves a queued child input through repeated pre-start checkpoints', async () => {
    const original = createActor(parent);
    original.getSnapshot().children.child!.send({ type: 'APPEND', value: 'A' });
    const saved = copy(original.getPersistedSnapshot());
    const unstarted = restoreActor(parent, saved);
    const resaved = copy(unstarted.getPersistedSnapshot());
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const actor = yield* createEffectActor(parent, { snapshot: resaved });
          yield* sendAndAwait(actor, { type: 'PING' });
          expect(
            actor.getSnapshot().children.child!.getSnapshot().context.value
          ).toBe('A');
        })
      )
    );
  });

  it('keeps restored child input ahead of newer pre-start input', async () => {
    const original = createActor(parent);
    original.getSnapshot().children.child!.send({ type: 'APPEND', value: 'A' });
    const unstarted = restoreActor(
      parent,
      copy(original.getPersistedSnapshot())
    );
    unstarted
      .getSnapshot()
      .children.child!.send({ type: 'APPEND', value: 'B' });
    const saved = copy(unstarted.getPersistedSnapshot());
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const actor = yield* createEffectActor(parent, { snapshot: saved });
          yield* sendAndAwait(actor, { type: 'PING' });
          expect(
            actor.getSnapshot().children.child!.getSnapshot().context.value
          ).toBe('AB');
        })
      )
    );
  });

  it('routes malformed child recovery data through the parent error transition', async () => {
    const machine = setup({ actors: { child } }).createMachine({
      initial: 'active',
      states: {
        active: {
          invoke: { id: 'child', src: 'child', onError: { target: 'failed' } }
        },
        failed: {}
      }
    });
    const original = createActor(machine);
    const nativeChild = original.getSnapshot().children.child!;
    const saved = copy({
      ...original.getPersistedSnapshot(),
      children: {
        child: {
          address: nativeChild.address,
          src: 'child',
          snapshot: {
            ...nativeChild.getPersistedSnapshot(),
            _xstateRecovery: {
              type: 'xstate.recovery',
              version: 1,
              mailbox: [null]
            }
          }
        }
      }
    });
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const actor = yield* createEffectActor(machine, { snapshot: saved });
          const recovered = yield* waitFor(actor, (snapshot) =>
            snapshot.matches('failed')
          );
          expect(recovered.status).toBe('active');
        })
      )
    );
  });
});

it('rejects an unavailable child incarnation until its pending stop is accepted', async () => {
  const worker = createMachine({
    context: { count: 0 },
    on: { INC: ({ context }) => ({ context: { count: context.count + 1 } }) }
  });
  const machine = setup({ actors: { worker } }).createMachine({
    context: { generation: 0 },
    initial: 'active',
    states: {
      active: {
        invoke: { id: 'worker', src: 'worker' },
        on: {
          RESET: {
            target: 'active',
            reenter: true,
            context: ({ context }) => ({ generation: context.generation + 1 })
          },
          INC: ({ children }, enq) =>
            enq.sendTo(children.worker!, { type: 'INC' }),
          PING: {}
        }
      }
    }
  });
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const original = yield* createEffectActor(machine);
        yield* sendAndAwait(original, { type: 'PING' });
        let captureError: unknown;
        original.subscribe((snapshot) => {
          if (snapshot.context.generation !== 1 || captureError) return;
          try {
            original.getPersistedSnapshot();
          } catch (error) {
            captureError = error;
          }
        });
        yield* sendAndAwait(original, { type: 'RESET' });
        expect(captureError).toBeInstanceOf(Error);
        const restored = yield* createEffectActor(machine, {
          snapshot: copy(original.getPersistedSnapshot())
        });
        yield* sendAndAwait(restored, { type: 'INC' });
        expect(
          restored.getSnapshot().children.worker!.getSnapshot().context.count
        ).toBe(1);
      })
    )
  );
});
