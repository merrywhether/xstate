import { Duration, Effect } from 'effect';
import { createMachine, setup } from 'xstate';
import { createEffectActor, waitFor } from './index.ts';
import { sendAndAwait } from './test.ts';

const copy = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;

it('restores a queued child snapshot for the restored child', async () => {
  const child = createMachine({
    context: { count: 0 },
    initial: 'ready',
    states: { ready: {} },
    on: { INC: ({ context }) => ({ context: { count: context.count + 1 } }) }
  });
  const machine = setup({ actors: { child } }).createMachine({
    context: { reports: 0, matched: 0 },
    invoke: {
      src: 'child',
      id: 'child',
      onSnapshot: ({ context, event }) => ({
        context: {
          reports: context.reports + 1,
          matched: context.matched + (event.snapshot.matches('ready') ? 1 : 0)
        }
      })
    },
    on: { PING: {} }
  });
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const original = yield* createEffectActor(machine);
        yield* sendAndAwait(original, { type: 'PING' });
        const before = original.getSnapshot().context.reports;
        original.getSnapshot().children.child!.send({ type: 'INC' });
        const saved = copy(original.getPersistedSnapshot());
        const restored = yield* createEffectActor(machine, { snapshot: saved });
        expect(restored.getSnapshot().children.child!.sessionId).not.toBe(
          original.getSnapshot().children.child!.sessionId
        );
        yield* waitFor(original, (s) => s.context.reports > before, {
          timeout: Duration.seconds(1)
        });
        yield* sendAndAwait(original, { type: 'PING' });
        const delivered = original.getSnapshot().context.reports;
        yield* waitFor(restored, (s) => s.context.reports >= delivered, {
          timeout: Duration.seconds(1)
        });
        expect(restored.getSnapshot().context.reports).toBe(delivered);
        expect(restored.getSnapshot().context.matched).toBe(delivered);
      })
    )
  );
});

it('restores an action failure queued before a later action captures', async () => {
  let capture = () => {};
  const machine = setup({
    actions: {
      fail: () => {
        throw new Error('expected failure');
      },
      capture: () => capture()
    }
  }).createMachine({
    context: { isError: false, message: '' },
    initial: 'active',
    states: {
      active: {
        on: {
          GO: (args, enq) => {
            enq(args.actions.fail);
            enq(args.actions.capture);
          },
          PING: {}
        },
        onError: ({ event }) => ({
          target: 'recovered',
          context: {
            isError: event.error instanceof Error,
            message: event.error instanceof Error ? event.error.message : ''
          }
        })
      },
      recovered: { on: { PING: {} } }
    }
  });
  let saved: ReturnType<typeof machine.getPersistedSnapshot> | undefined;
  let captureError: unknown;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const original = yield* createEffectActor(machine);
        yield* sendAndAwait(original, { type: 'PING' });
        capture = () => {
          try {
            saved = copy(original.getPersistedSnapshot());
          } catch (error) {
            captureError = error;
          }
        };
        yield* sendAndAwait(original, { type: 'GO' });
        expect(captureError).toBeUndefined();
        expect(saved).toBeDefined();
        yield* sendAndAwait(original, { type: 'PING' });
        expect(original.getSnapshot().value).toBe('recovered');
        if (!saved) throw new Error('capture did not run');
        const restored = yield* createEffectActor(machine, { snapshot: saved });
        yield* waitFor(restored, (s) => s.value === 'recovered', {
          timeout: Duration.seconds(1)
        });
        expect(restored.getSnapshot().context).toEqual({
          isError: true,
          message: 'expected failure'
        });
      })
    )
  );
});
