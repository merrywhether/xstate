import { Context, Duration, Effect, Schema } from 'effect';
import { createActor, createMachine, machineVersions } from 'xstate';
import type { PersistedSnapshotOf } from 'xstate';
import {
  createEffectActor,
  createSnapshotCodec,
  SnapshotCodecError,
  waitFor
} from './index.ts';
import { sendAndAwait } from './test.ts';

const machine = createMachine({
  id: 'counter',
  version: '2',
  context: { count: 0 },
  initial: 'idle',
  states: { idle: {} }
});
const fields = {
  status: Schema.Literals(['active', 'done', 'stopped', 'error']),
  value: Schema.Unknown,
  context: Schema.Struct({ count: Schema.Number }),
  children: Schema.Record(Schema.String, Schema.Unknown),
  timers: Schema.Array(Schema.Unknown),
  historyValue: Schema.Record(Schema.String, Schema.Unknown),
  _nextTimerId: Schema.Number,
  machine: Schema.Struct({
    id: Schema.Literal('counter'),
    version: Schema.String
  })
};
const SnapshotSchema = Schema.Struct(fields);
const codec = createSnapshotCodec(machine, SnapshotSchema);
const persisted = () => createActor(machine).getPersistedSnapshot();

it('round-trips an actual machine snapshot and restores it', async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(machine);
        const json = yield* codec.encode(actor.getPersistedSnapshot());
        const snapshot = yield* codec.decode(json);
        const restored = yield* createEffectActor(machine, { snapshot });
        expect(restored.getSnapshot().value).toBe('idle');
        expect(restored.getSnapshot().context).toEqual({ count: 0 });
        expect(snapshot.machine).toEqual({ id: 'counter', version: '2' });
      })
    )
  );
});

it('retains rich values through explicit bidirectional schemas', async () => {
  const rich = createMachine({
    id: 'counter',
    version: '2',
    context: { when: new Date('2026-01-01'), size: 42n },
    initial: 'idle',
    states: { idle: {} }
  });
  const richCodec = createSnapshotCodec(
    rich,
    Schema.Struct({
      ...fields,
      context: Schema.Struct({
        when: Schema.DateFromString,
        size: Schema.BigIntFromString
      })
    })
  );
  const snapshot = createActor(rich).getPersistedSnapshot();
  const decoded = await Effect.runPromise(
    Effect.flatMap(richCodec.encode(snapshot), richCodec.decode)
  );
  expect(decoded.context).toEqual(snapshot.context);
});

it.each([
  undefined,
  NaN,
  Infinity,
  -0,
  42n,
  () => {},
  Symbol('value'),
  new Date(),
  new Map(),
  Object.assign([1], { extra: true }),
  new Array(1),
  Object.defineProperty([1], 'toJSON', { value: () => [] }),
  Object.defineProperty([1], '0', { get: () => 1, enumerable: true }),
  new (class extends Array {})()
])('rejects unsupported encoded JSON values', async (value) => {
  const rawCodec = createSnapshotCodec(
    machine,
    Schema.Struct({ ...fields, context: Schema.Unknown })
  );
  const result = await Effect.runPromise(
    Effect.flip(rawCodec.encode({ ...persisted(), context: value }))
  );
  expect(result).toBeInstanceOf(SnapshotCodecError);
});

it('rejects cycles with a typed path', async () => {
  const cyclic: { child?: unknown } = {};
  cyclic.child = cyclic;
  const rawCodec = createSnapshotCodec(
    machine,
    Schema.Struct({ ...fields, context: Schema.Unknown })
  );
  const result = await Effect.runPromise(
    Effect.flip(rawCodec.encode({ ...persisted(), context: cyclic }))
  );
  expect(result).toMatchObject({
    _tag: 'SnapshotCodecError',
    path: '$.context.child'
  });
});

it.each([
  'invalid json',
  JSON.stringify({ format: '@xstate/effect/snapshot', version: 2 }),
  JSON.stringify({
    format: '@xstate/effect/snapshot',
    version: 1,
    logicId: 'wrong'
  })
])('rejects malformed, future and unrelated envelopes', async (json) => {
  expect(
    await Effect.runPromise(Effect.flip(codec.decode(json)))
  ).toBeInstanceOf(SnapshotCodecError);
});

it('preserves core metadata and rejects schemas that discard it', async () => {
  const metadata = {
    ...persisted(),
    stateInputs: { idle: { value: 1 } },
    _nextActorIds: { worker: 4 },
    extraMetadata: { retained: true }
  };
  const complete = createSnapshotCodec(
    machine,
    Schema.Struct({
      ...fields,
      stateInputs: Schema.Unknown,
      _nextActorIds: Schema.Unknown,
      extraMetadata: Schema.Unknown
    })
  );
  const decoded = await Effect.runPromise(
    Effect.flatMap(complete.encode(metadata), complete.decode)
  );
  expect(decoded.stateInputs).toEqual(metadata.stateInputs);
  expect(decoded._nextActorIds).toEqual(metadata._nextActorIds);
  expect(decoded.extraMetadata).toEqual(metadata.extraMetadata);
  expect(
    await Effect.runPromise(Effect.flip(codec.encode(metadata)))
  ).toMatchObject({ _tag: 'SnapshotCodecError' });
});

it('rejects failed roots and embedded children rather than losing Causes', async () => {
  for (const snapshot of [
    {
      ...persisted(),
      status: 'error' as const,
      output: undefined,
      error: new Error('failed')
    },
    {
      ...persisted(),
      children: {
        worker: {
          src: 'worker',
          snapshot: { status: 'error', error: { _tag: 'EffectFailure' } }
        }
      }
    }
  ]) {
    expect(
      await Effect.runPromise(Effect.flip(codec.encode(snapshot)))
    ).toMatchObject({ _tag: 'SnapshotCodecError' });
  }
});

it('preserves historical versions until the existing migration owner restores them', async () => {
  const old = createMachine({
    id: 'counter',
    version: '1',
    context: { count: 1 },
    initial: 'idle',
    states: { idle: {} }
  });
  const versions = machineVersions([old, machine]);
  const json = await Effect.runPromise(
    codec.encode(createActor(old).getPersistedSnapshot())
  );
  const decoded = await Effect.runPromise(codec.decode(json));
  expect(decoded.machine.version).toBe('1');
  const migrated = await versions.migrateSnapshot(decoded, {
    to: '2',
    migrations: {
      '1': (snapshot) => ({
        ...snapshot,
        context: { count: snapshot.context.count + 1 }
      })
    }
  });
  expect(migrated.context).toEqual({ count: 2 });
  expect(migrated.machine.version).toBe('2');
});

it('retains decoding and encoding service requirements', () => {
  const Service = Context.Service<{ value: string }>('SnapshotService');
  const requiring: Schema.Codec<
    typeof SnapshotSchema.Type,
    typeof SnapshotSchema.Encoded,
    typeof Service,
    typeof Service
  > = SnapshotSchema;
  const requiringCodec = createSnapshotCodec(machine, requiring);
  const invalid = () => {
    // @ts-expect-error -- decoding still needs the schema service
    Effect.runPromise(requiringCodec.decode(''));
    // @ts-expect-error -- encoding still needs the schema service
    Effect.runPromise(requiringCodec.encode(persisted()));
    // @ts-expect-error -- only versioned machines have stable codec identity
    createSnapshotCodec(createMachine({}), SnapshotSchema);
    createSnapshotCodec(
      createMachine({ id: 'different', version: '1' }),
      // @ts-expect-error -- unrelated machine identity
      SnapshotSchema
    );
    const decoder: Schema.Decoder<typeof SnapshotSchema.Type, never> =
      SnapshotSchema;
    // @ts-expect-error -- encoding requires a complete bidirectional schema
    createSnapshotCodec(machine, decoder);
  };
  void invalid;
});

it('rejects dropping only state inputs', async () => {
  const snapshot = {
    ...persisted(),
    stateInputs: { idle: { accepted: true } }
  };
  expect(
    await Effect.runPromise(Effect.flip(codec.encode(snapshot)))
  ).toMatchObject({
    _tag: 'SnapshotCodecError',
    path: '$.snapshot.stateInputs'
  });
});

it('round-trips embedded machine children with pending timer deadlines', async () => {
  const { setup, types } = await import('xstate');
  const child = createMachine({
    id: 'child',
    version: '1',
    context: { count: 1 },
    initial: 'waiting',
    states: {
      waiting: { after: { 60000: { target: 'done' } } },
      done: { type: 'final' }
    }
  });
  const parent = setup({
    actors: { child },
    schemas: { context: types<{ count: number }>() }
  }).createMachine({
    id: 'counter',
    version: '2',
    context: { count: 0 },
    initial: 'idle',
    states: { idle: { invoke: { src: 'child' } } }
  });
  const Child = Schema.Struct({
    ...fields,
    machine: Schema.Struct({
      id: Schema.Literal('child'),
      version: Schema.Literal('1')
    })
  });
  const nested = createSnapshotCodec(
    parent,
    Schema.Struct({
      ...fields,
      children: Schema.Record(
        Schema.String,
        Schema.Struct({
          src: Schema.String,
          address: Schema.String,
          syncSnapshot: Schema.optionalKey(Schema.Boolean),
          snapshot: Child
        })
      )
    })
  );
  const actor = createActor(parent).start();
  try {
    const original = actor.getPersistedSnapshot();
    const restored = await Effect.runPromise(
      Effect.flatMap(nested.encode(original), nested.decode)
    );
    const before = Object.values(original.children)[0].snapshot;
    const after = Object.values(restored.children)[0].snapshot;
    expect(after).toMatchObject({
      machine: { id: 'child', version: '1' },
      value: 'waiting'
    });
    expect(after).toEqual(JSON.parse(JSON.stringify(before)));
  } finally {
    actor.stop();
  }
});

it.each(['task', 'stream'] as const)(
  'round-trips rich input of an embedded active %s',
  async (kind) => {
    const { setup, types } = await import('xstate');
    const { Stream } = await import('effect');
    const { createEffectLogic, createEffectStreamLogic } =
      await import('./index.ts');
    const worker =
      kind === 'task'
        ? createEffectLogic((_args: { input: { when: Date } }) => Effect.never)
        : createEffectStreamLogic(
            (_args: { input: { when: Date } }) => Stream.never
          );
    const parent = setup({
      actors: { worker },
      schemas: { context: types<{ count: number }>() }
    }).createMachine({
      id: 'counter',
      version: '2',
      context: { count: 0 },
      initial: 'idle',
      states: {
        idle: {
          invoke: { src: 'worker', input: { when: new Date('2026-01-01') } }
        }
      }
    });
    const Child = Schema.Struct({
      status: Schema.Literal('active'),
      input: Schema.Struct({ when: Schema.DateFromString })
    });
    const nested = createSnapshotCodec(
      parent,
      Schema.Struct({
        ...fields,
        children: Schema.Record(
          Schema.String,
          Schema.Struct({
            src: Schema.String,
            address: Schema.String,
            syncSnapshot: Schema.optionalKey(Schema.Boolean),
            snapshot: Child
          })
        )
      })
    );
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const actor = yield* createEffectActor(parent);
          const decoded = yield* Effect.flatMap(
            nested.encode(actor.getPersistedSnapshot()),
            nested.decode
          );
          expect(
            Object.values(decoded.children)[0].snapshot.input.when
          ).toEqual(new Date('2026-01-01'));
          const restored = yield* createEffectActor(parent, {
            snapshot: decoded
          });
          expect(
            Object.values(restored.getSnapshot().children)[0]?.getSnapshot()
              .input.when
          ).toEqual(new Date('2026-01-01'));
        })
      )
    );
  }
);

it('restores inline machine children whose native descriptors omit src', async () => {
  const child = createMachine({
    id: 'child',
    version: '1',
    context: { count: 1 },
    initial: 'waiting',
    states: { waiting: {} }
  });
  const parent = createMachine({
    id: 'counter',
    version: '2',
    context: { count: 0 },
    initial: 'idle',
    states: { idle: { invoke: { src: child } } }
  });
  const Child = Schema.Struct({
    ...fields,
    machine: Schema.Struct({
      id: Schema.Literal('child'),
      version: Schema.Literal('1')
    })
  });
  const nested = createSnapshotCodec(
    parent,
    Schema.Struct({
      ...fields,
      children: Schema.Record(
        Schema.String,
        Schema.Struct({
          src: Schema.optionalKey(Schema.String),
          address: Schema.String,
          syncSnapshot: Schema.optionalKey(Schema.Boolean),
          snapshot: Child
        })
      )
    })
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(parent);
        const original = actor.getPersistedSnapshot();
        expect(Object.values(original.children)[0]).not.toHaveProperty('src');
        const decoded = yield* Effect.flatMap(
          nested.encode(original),
          nested.decode
        );
        expect(Object.values(decoded.children)[0]).not.toHaveProperty('src');
        const restored = yield* createEffectActor(parent, {
          snapshot: decoded
        });
        expect(
          Object.values(restored.getSnapshot().children)[0]?.getSnapshot().value
        ).toBe('waiting');
      })
    )
  );
});

it('preserves queued root events through a schema that omits recovery metadata', async () => {
  const logic = createMachine({
    id: 'counter',
    version: '2',
    context: { count: 0 },
    initial: 'idle',
    states: { idle: {} },
    on: {
      PING: {},
      INC: ({ context }) => ({ context: { count: context.count + 1 } })
    }
  });
  const strict = createSnapshotCodec(logic, Schema.Struct(fields));
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const original = yield* createEffectActor(logic);
        yield* sendAndAwait(original, { type: 'PING' });
        original.send({ type: 'INC' });
        const json = yield* strict.encode(original.getPersistedSnapshot());
        const snapshot = yield* strict.decode(json);
        const restored = yield* createEffectActor(logic, { snapshot });
        yield* waitFor(restored, (s) => s.context.count === 1, {
          timeout: Duration.seconds(1)
        });
        expect(restored.getSnapshot().context.count).toBe(1);
      })
    )
  );
});

it('preserves nested child and root continuations through strict snapshot schemas', async () => {
  const child = createMachine({
    id: 'child',
    version: '1',
    context: { count: 0 },
    initial: 'ready',
    states: { ready: {} },
    on: {
      INC: ({ context, parent }, enq) => {
        enq.sendTo(parent!, { type: 'REPORT' });
        return { context: { count: context.count + 1 } };
      }
    }
  });
  const logic = createMachine({
    id: 'counter',
    version: '2',
    actors: { child },
    context: { count: 0 },
    initial: 'idle',
    states: { idle: { invoke: { src: 'child', id: 'child' } } },
    on: {
      PING: {},
      GO: ({ children }, enq) => {
        enq.sendTo(children.child!, { type: 'INC' });
        enq.sendTo(children.child!, { type: 'INC' });
      },
      REPORT: ({ context }) => ({ context: { count: context.count + 1 } })
    }
  });
  const childSchema = Schema.Struct({
    ...fields,
    machine: Schema.Struct({
      id: Schema.Literal('child'),
      version: Schema.Literal('1')
    })
  });
  const strict = createSnapshotCodec(
    logic,
    Schema.Struct({
      ...fields,
      children: Schema.Record(
        Schema.String,
        Schema.Struct({
          src: Schema.String,
          address: Schema.String,
          syncSnapshot: Schema.optionalKey(Schema.Boolean),
          snapshot: childSchema
        })
      )
    })
  );
  let snapshot: PersistedSnapshotOf<typeof logic> | undefined;
  let captureError: unknown;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const original = yield* createEffectActor(logic);
        yield* sendAndAwait(original, { type: 'PING' });
        original.getSnapshot().children.child!.subscribe((s) => {
          if (s.context.count === 1 && !snapshot && !captureError) {
            try {
              snapshot = original.getPersistedSnapshot();
            } catch (error) {
              captureError = error;
            }
          }
        });
        yield* sendAndAwait(original, { type: 'GO' });
        expect(captureError).toBeUndefined();
        expect(snapshot).toBeDefined();
        if (!snapshot) throw new Error('snapshot was not captured');
        const json = yield* strict.encode(snapshot);
        const decoded = yield* strict.decode(json);
        const restored = yield* createEffectActor(logic, { snapshot: decoded });
        yield* waitFor(restored, (s) => s.context.count === 2, {
          timeout: Duration.seconds(1)
        });
        expect(
          restored.getSnapshot().children.child!.getSnapshot().context.count
        ).toBe(2);
      })
    )
  );
});
