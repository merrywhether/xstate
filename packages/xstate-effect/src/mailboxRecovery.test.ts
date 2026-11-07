import { Effect } from 'effect';
import { createMachine } from 'xstate';
import { createEffectActor, waitFor } from './index.ts';
import { sendAndAwait } from './test.ts';

const copy = <T>(snapshot: T): T => JSON.parse(JSON.stringify(snapshot));

it('mailbox metadata restores an accepted external event', async () => {
  const machine = createMachine({
    context: { count: 0 },
    on: {
      INC: ({ context }) => ({ context: { count: context.count + 1 } }),
      PING: () => {}
    }
  });
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const original = yield* createEffectActor(machine);
        yield* sendAndAwait(original, { type: 'PING' });
        original.send({ type: 'INC' });
        const saved = copy(original.getPersistedSnapshot());
        const restored = yield* createEffectActor(machine, { snapshot: saved });
        yield* sendAndAwait(restored, { type: 'PING' });
        expect(restored.getSnapshot().context.count).toBe(1);
      })
    )
  );
});

it.each(['send', 'sendAndAwait'] as const)(
  'restores parent/child progress after %s and an immediate checkpoint',
  async (flow) => {
    const child = createMachine({
      initial: 'working',
      states: {
        working: { on: { WORK: { target: 'done' } } },
        done: { type: 'final' }
      }
    });
    const machine = createMachine({
      actors: { child },
      context: { reports: 0 },
      invoke: {
        id: 'child',
        src: 'child',
        onDone: ({ context }) => ({ context: { reports: context.reports + 1 } })
      },
      on: {
        GO: (_, enq) => enq.sendTo('child', { type: 'WORK' })
      }
    });
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const original = yield* createEffectActor(machine);
          if (flow === 'send') {
            original.send({ type: 'GO' });
            expect(original.getSnapshot().context.reports).toBe(0);
          } else {
            yield* sendAndAwait(original, { type: 'GO' });
          }
          const saved = copy(original.getPersistedSnapshot());
          const restored = yield* createEffectActor(machine, {
            snapshot: saved
          });
          const uninterrupted = yield* waitFor(
            original,
            (snapshot) => snapshot.context.reports === 1
          );
          const recovered = yield* waitFor(
            restored,
            (snapshot) => snapshot.context.reports === 1
          );
          expect(recovered.context.reports).toBe(1);
          expect(recovered.context.reports).toBe(uninterrupted.context.reports);
        })
      )
    );
  }
);

it('restores a queued self-send after acknowledging its triggering event', async () => {
  const machine = createMachine({
    initial: 'idle',
    states: {
      idle: {
        on: {
          GO: ({ self }, enq) => {
            enq.sendTo(self, { type: 'CONTINUE' });
            return { target: 'waiting' };
          }
        }
      },
      waiting: { on: { CONTINUE: { target: 'complete' } } },
      complete: { type: 'final' }
    }
  });
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const original = yield* createEffectActor(machine);
        yield* sendAndAwait(original, { type: 'GO' });
        const saved = copy(original.getPersistedSnapshot());
        const restored = yield* createEffectActor(machine, { snapshot: saved });
        const uninterrupted = yield* waitFor(
          original,
          (snapshot) => snapshot.status === 'done',
          {
            timeout: '1 second'
          }
        );
        const recovered = yield* waitFor(
          restored,
          (snapshot) => snapshot.status === 'done',
          {
            timeout: '1 second'
          }
        );
        expect(recovered.value).toBe('complete');
        expect(recovered.value).toBe(uninterrupted.value);
      })
    )
  );
});

it('does not let a queued completion finish a replacement child with the same id', async () => {
  const child = createMachine({
    initial: 'working',
    states: {
      working: { on: { WORK: { target: 'done' } } },
      done: { type: 'final' }
    }
  });
  const machine = createMachine({
    actors: { child },
    context: { reports: 0, resets: 0 },
    initial: 'active',
    states: {
      active: {
        invoke: {
          id: 'child',
          src: 'child',
          onDone: ({ context }) => ({
            context: { ...context, reports: context.reports + 1 }
          })
        },
        on: {
          RESET: {
            target: 'active',
            reenter: true,
            context: ({ context }) => ({
              ...context,
              resets: context.resets + 1
            })
          }
        }
      }
    },
    on: {
      GO: (_, enq) => enq.sendTo('child', { type: 'WORK' }),
      PING: () => {}
    }
  });
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const original = yield* createEffectActor(machine);
        yield* sendAndAwait(original, { type: 'PING' });
        original.getSnapshot().children.child!.subscribe({
          complete: () => original.send({ type: 'RESET' })
        });
        yield* sendAndAwait(original, { type: 'GO' });
        const saved = copy(original.getPersistedSnapshot());
        const restored = yield* createEffectActor(machine, { snapshot: saved });
        yield* sendAndAwait(original, { type: 'PING' });
        yield* sendAndAwait(restored, { type: 'PING' });
        expect(restored.getSnapshot().context).toEqual({
          reports: 0,
          resets: 1
        });
        expect(
          restored.getSnapshot().children.child!.getSnapshot().status
        ).toBe('active');
        expect(original.getSnapshot().context).toEqual(
          restored.getSnapshot().context
        );
      })
    )
  );
});
