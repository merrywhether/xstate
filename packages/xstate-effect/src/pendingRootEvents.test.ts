import { Effect, Schema, Scope } from 'effect';
import {
  createActor,
  createAsyncLogic,
  createMachine,
  setup,
  types,
  type AnyActorLogic,
  type PersistedSnapshotOf
} from 'xstate';
import { restoreActor } from 'xstate/persist';
import {
  createEffectActor,
  createSnapshotCodec,
  sendAndAwait,
  waitFor,
  type EffectActor
} from './index.ts';

const run = <A, E>(effect: Effect.Effect<A, E, Scope.Scope>) =>
  Effect.runPromise(Effect.scoped(effect));

const capture = <TLogic extends AnyActorLogic>(actor: EffectActor<TLogic>) =>
  Effect.sync(() => actor.getPersistedSnapshot());

const roundTrip = <TSnapshot>(snapshot: TSnapshot): TSnapshot =>
  JSON.parse(JSON.stringify(snapshot));

const child = createMachine({
  initial: 'working',
  states: {
    working: { on: { WORK: { target: 'done' } } },
    done: { type: 'final' }
  }
});

const parent = createMachine({
  actors: { child },
  initial: 'working',
  on: { PING: () => {} },
  states: {
    working: {
      invoke: { id: 'child', src: 'child', onDone: { target: 'success' } },
      on: { GO: (_, enq) => enq.sendTo('child', { type: 'WORK' }) }
    },
    success: {}
  }
});

const createFetchMachine = (load: () => Promise<{ value: number }>) =>
  setup({
    actors: { fetch: createAsyncLogic({ run: load }) },
    schemas: { context: types<{ value: number }>() }
  }).createMachine({
    context: { value: 0 },
    initial: 'idle',
    on: { PING: () => {} },
    states: {
      idle: { on: { GO: { target: 'loading' } } },
      loading: {
        invoke: {
          id: 'fetch',
          src: 'fetch',
          onDone: ({ event }) => ({
            target: 'success',
            context: { value: event.output.value }
          })
        }
      },
      success: {}
    }
  });

const createDateFixture = () => {
  const date = new Date('2026-01-01T00:00:00Z');
  const fetch = createAsyncLogic({
    run: async () => ({ when: date.toISOString() })
  });
  const machine = setup({
    actors: { fetch },
    schemas: { context: types<{ when: Date }>() }
  }).createMachine({
    id: 'rich',
    version: '1',
    context: { when: new Date(0) },
    initial: 'idle',
    on: { PING: () => {} },
    states: {
      idle: { on: { GO: { target: 'loading' } } },
      loading: {
        invoke: {
          src: 'fetch',
          onDone: ({ event }) => ({
            target: 'ready',
            context: { when: new Date(event.output.when) }
          })
        }
      },
      ready: {}
    }
  });
  const codec = createSnapshotCodec(
    machine,
    Schema.Struct({
      status: Schema.Literals(['active', 'done', 'stopped', 'error']),
      value: Schema.Unknown,
      context: Schema.Struct({ when: Schema.DateFromString }),
      children: Schema.Record(
        Schema.String,
        Schema.Struct({
          src: Schema.String,
          address: Schema.String,
          syncSnapshot: Schema.optionalKey(Schema.Boolean),
          snapshot: Schema.Struct({
            status: Schema.Literal('done'),
            output: Schema.Struct({ when: Schema.String }),
            effects: Schema.Unknown
          })
        })
      ),
      timers: Schema.Array(Schema.Unknown),
      historyValue: Schema.Record(Schema.String, Schema.Unknown),
      nextTimerId: Schema.Number,
      machine: Schema.Struct({
        id: Schema.Literal('rich'),
        version: Schema.String
      })
    })
  );
  const encodeAndDecode = (snapshot: PersistedSnapshotOf<typeof machine>) =>
    Effect.flatMap(codec.encode(snapshot), codec.decode);
  return { date, machine, encodeAndDecode };
};

describe('restoring an actor with pending root events', () => {
  // #5892: Both sends can finish the child before the parent handles onDone.
  it.fails.each(['parent event', 'direct child send'] as const)(
    'restores a child completion after a %s',
    async (delivery) => {
      await run(
        Effect.gen(function* () {
          const actor = yield* createEffectActor(parent);
          if (delivery === 'parent event') {
            yield* sendAndAwait(actor, { type: 'GO' });
          } else {
            yield* sendAndAwait(actor, { type: 'PING' });
            actor.getSnapshot().children.child!.send({ type: 'WORK' });
          }
          const snapshot = roundTrip(yield* capture(actor));
          yield* waitFor(actor, (state) => state.matches('success'), {
            timeout: '1 second'
          });

          const restored = yield* createEffectActor(parent, { snapshot });
          const state = yield* sendAndAwait(restored, { type: 'PING' });
          expect(state.value).toBe('success');
        })
      );
    }
  );

  // #5892: An immediately resolved invocation can also leave a queued reply.
  it.fails('restores the result of an immediately resolved fetch', async () => {
    const machine = createFetchMachine(() => Promise.resolve({ value: 42 }));
    await run(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(machine);
        yield* sendAndAwait(actor, { type: 'GO' });
        const snapshot = roundTrip(yield* capture(actor));
        yield* waitFor(actor, (state) => state.matches('success'), {
          timeout: '1 second'
        });

        const restored = yield* createEffectActor(machine, { snapshot });
        const state = yield* sendAndAwait(restored, { type: 'PING' });
        expect(state.value).toBe('success');
        expect(state.context.value).toBe(42);
      })
    );
  });

  it('restarts a fetch that was still active at capture', async () => {
    const original = Promise.withResolvers<{ value: number }>();
    const restarted = Promise.withResolvers<{ value: number }>();
    const load = vi
      .fn(() => restarted.promise)
      .mockReturnValueOnce(original.promise);
    const machine = createFetchMachine(load);
    await run(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(machine);
        yield* sendAndAwait(actor, { type: 'GO' });
        const snapshot = roundTrip(yield* capture(actor));
        expect(snapshot.value).toBe('loading');
        expect(actor.getSnapshot().children.fetch?.getSnapshot().status).toBe(
          'active'
        );

        const restored = yield* createEffectActor(machine, { snapshot });
        restarted.resolve({ value: 42 });
        const state = yield* waitFor(restored, (s) => s.matches('success'), {
          timeout: '1 second'
        });
        expect(state.context.value).toBe(42);
        expect(load).toHaveBeenCalledTimes(2);
        expect(actor.getSnapshot().value).toBe('loading');
      })
    );
  });

  it('restores the native parent after send handles the child completion', () => {
    const actor = createActor(parent).start();
    actor.send({ type: 'GO' });
    const restored = restoreActor(
      parent,
      roundTrip(actor.getPersistedSnapshot())
    ).start();
    try {
      expect(actor.getSnapshot().value).toBe('success');
      expect(restored.getSnapshot().value).toBe('success');
    } finally {
      restored.stop();
      actor.stop();
    }
  });

  // #5892: Restoring the completion also needs to preserve its rich result.
  it.fails('restores a fetched Date through the snapshot codec', async () => {
    const { date, machine, encodeAndDecode } = createDateFixture();

    await run(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(machine);
        yield* sendAndAwait(actor, { type: 'GO' });
        const snapshot = yield* encodeAndDecode(yield* capture(actor));
        yield* waitFor(actor, (state) => state.matches('ready'), {
          timeout: '1 second'
        });

        const restored = yield* createEffectActor(machine, { snapshot });
        const state = yield* sendAndAwait(restored, { type: 'PING' });
        expect(state.value).toBe('ready');
        expect(state.context.when).toEqual(date);
      })
    );
  });

  it('round-trips a settled snapshot with a Date in context', async () => {
    const { date, machine, encodeAndDecode } = createDateFixture();
    await run(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(machine);
        yield* sendAndAwait(actor, { type: 'GO' });
        yield* waitFor(actor, (state) => state.matches('ready'), {
          timeout: '1 second'
        });
        yield* sendAndAwait(actor, { type: 'PING' });
        const snapshot = yield* encodeAndDecode(yield* capture(actor));

        const restored = yield* createEffectActor(machine, { snapshot });
        expect(restored.getSnapshot().value).toBe('ready');
        expect(restored.getSnapshot().context.when).toEqual(date);
      })
    );
  });

  // #5892: A queued execution error can be lost just like a child reply.
  it.fails('restores the recovery transition after an action fails', async () => {
    const machine = createMachine({
      initial: 'idle',
      on: { PING: () => {} },
      onError: { target: '.recovered' },
      states: {
        idle: {
          on: {
            GO: (_, enq) => {
              enq(() => {
                throw new Error('action failed');
              });
              return { target: 'working' };
            }
          }
        },
        working: {},
        recovered: {}
      }
    });
    await run(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(machine);
        yield* sendAndAwait(actor, { type: 'GO' });
        const snapshot = roundTrip(yield* capture(actor));
        yield* waitFor(actor, (state) => state.matches('recovered'), {
          timeout: '1 second'
        });

        const restored = yield* createEffectActor(machine, { snapshot });
        const state = yield* sendAndAwait(restored, { type: 'PING' });
        expect(state.value).toBe('recovered');
      })
    );
  });
});
