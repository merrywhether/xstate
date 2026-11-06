import {
  Cause,
  Clock,
  Duration,
  Effect,
  Exit,
  Fiber,
  Layer,
  References,
  Scope
} from 'effect';
import { TestClock } from 'effect/testing';
import { createMachine } from 'xstate';
import {
  ActorStoppedError,
  checkpoint,
  createEffectActor,
  createEffectLogic,
  setupEffect
} from './index.ts';
import { sendAndAwait } from './test.ts';

const run = <A, E>(effect: Effect.Effect<A, E, Scope.Scope>) =>
  Effect.runPromise(Effect.scoped(effect));

const child = createMachine({
  initial: 'working',
  states: {
    working: { on: { WORK: { target: 'done' } } },
    done: { type: 'final' }
  }
});
const parent = createMachine({
  actors: { child },
  context: { reports: 0, extra: 0 },
  invoke: {
    id: 'child',
    src: 'child',
    onDone: ({ context }) => ({
      context: { ...context, reports: context.reports + 1 }
    })
  },
  on: {
    GO: (_, enq) => enq.sendTo('child', { type: 'WORK' }),
    EXTRA: ({ context }) => ({
      context: { ...context, extra: context.extra + 1 }
    })
  }
});

const json = JSON.stringify;

describe('checkpoint', () => {
  it.each([undefined, 5])(
    'includes child completion and reentrant events across restore (operation budget %s)',
    async (budget) => {
      let program = Effect.gen(function* () {
        const actor = yield* createEffectActor(parent);
        const subscription = actor.subscribe((snapshot) => {
          if (snapshot.context.reports === 1 && snapshot.context.extra === 0) {
            actor.send({ type: 'EXTRA' });
          }
        });
        yield* sendAndAwait(actor, { type: 'GO' });
        const saved = JSON.parse(yield* checkpoint(actor, json));
        expect(saved.context).toEqual({ reports: 1, extra: 1 });
        const restored = yield* createEffectActor(parent, { snapshot: saved });
        const restoredSaved = JSON.parse(yield* checkpoint(restored, json));
        expect(restoredSaved.context).toEqual({ reports: 1, extra: 1 });
        subscription.unsubscribe();
      });
      if (budget !== undefined) {
        program = program.pipe(
          Effect.provideService(References.MaxOpsBeforeYield, budget)
        );
      }
      await run(program);
    }
  );

  it('includes a child that completes during initialization', async () => {
    const done = createMachine({
      initial: 'done',
      states: { done: { type: 'final' } }
    });
    const machine = createMachine({
      actors: { done },
      context: { reports: 0 },
      invoke: {
        src: 'done',
        onDone: ({ context }) => ({ context: { reports: context.reports + 1 } })
      }
    });
    await run(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(machine);
        expect(JSON.parse(yield* checkpoint(actor, json)).context.reports).toBe(
          1
        );
      })
    );
  });

  it('drains multiple child replies before capturing', async () => {
    const responder = createMachine({
      on: { PING: ({ parent }, enq) => enq.sendTo(parent!, { type: 'PONG' }) }
    });
    const machine = createMachine({
      actors: { responder },
      context: { replies: 0 },
      invoke: { id: 'responder', src: 'responder' },
      on: {
        START: (_, enq) => enq.sendTo('responder', { type: 'PING' }),
        PONG: ({ context }, enq) => {
          const replies = context.replies + 1;
          if (replies < 3) enq.sendTo('responder', { type: 'PING' });
          return { context: { replies } };
        }
      }
    });
    await run(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(machine);
        actor.send({ type: 'START' });
        expect(JSON.parse(yield* checkpoint(actor, json)).context.replies).toBe(
          3
        );
      })
    );
  });

  it('drains events sent by one encoder before a concurrent capture', async () => {
    await run(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(parent);
        const first = yield* Effect.forkChild(
          checkpoint(actor, (snapshot) => {
            actor.send({ type: 'EXTRA' });
            return json(snapshot);
          }),
          { startImmediately: true }
        );
        const second = yield* Effect.forkChild(checkpoint(json)(actor), {
          startImmediately: true
        });
        expect(JSON.parse(yield* Fiber.join(first)).context.extra).toBe(0);
        expect(JSON.parse(yield* Fiber.join(second)).context.extra).toBe(1);
      })
    );
  });

  it('does not run an interrupted encoder and permits a later checkpoint', async () => {
    await run(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(parent);
        let calls = 0;
        const fiber = yield* Effect.forkChild(
          checkpoint(actor, () => {
            calls++;
            return 'cancelled';
          }),
          { startImmediately: true }
        );
        yield* Fiber.interrupt(fiber);
        expect(JSON.parse(yield* checkpoint(actor, json)).context.reports).toBe(
          0
        );
        expect(calls).toBe(0);
      })
    );
  });

  it('reports encoder throws and non-string JavaScript results as failures', async () => {
    await run(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(parent);
        const thrown = new Error('cannot serialize');
        const failure = yield* Effect.flip(
          checkpoint(actor, () => {
            throw thrown;
          })
        );
        expect(failure).toBeInstanceOf(Cause.UnknownError);
        expect(failure).toMatchObject({ cause: thrown });
        const identity = yield* Effect.flip(
          checkpoint(actor, ((snapshot: unknown) => snapshot) as never)
        );
        const asyncEncoder = yield* Effect.flip(
          checkpoint(actor, (async () => '') as never)
        );
        expect(identity).toBeInstanceOf(Cause.UnknownError);
        expect(asyncEncoder).toBeInstanceOf(Cause.UnknownError);
        expect(JSON.parse(yield* checkpoint(actor, json)).context.reports).toBe(
          0
        );
      })
    );
  });

  it('fails after its scope stops the actor', async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const scope = yield* Scope.make();
        const actor = yield* Scope.provide(createEffectActor(parent), scope);
        yield* Scope.close(scope, Exit.void);
        expect(yield* Effect.flip(checkpoint(actor, json))).toBeInstanceOf(
          ActorStoppedError
        );
      })
    );
  });

  it('keeps encoded child state detached from later changes', async () => {
    const counter = createMachine({
      context: { n: 0 },
      on: { INC: ({ context }) => ({ context: { n: context.n + 1 } }) }
    });
    const machine = createMachine({
      actors: { counter },
      invoke: { id: 'counter', src: 'counter' },
      on: { INC: (_, enq) => enq.sendTo('counter', { type: 'INC' }) }
    });
    await run(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(machine);
        const saved = JSON.parse(yield* checkpoint(actor, json));
        yield* sendAndAwait(actor, { type: 'INC' });
        const newer = JSON.parse(yield* checkpoint(actor, json));
        expect(saved.children.counter.snapshot.context.n).toBe(0);
        expect(newer.children.counter.snapshot.context.n).toBe(1);
      })
    );
  });

  it('does not wait for a running Effect task', async () => {
    const worker = createEffectLogic(Effect.never);
    const machine = createMachine({
      actors: { worker },
      invoke: { id: 'worker', src: 'worker' }
    });
    await run(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(machine);
        expect(
          JSON.parse(yield* checkpoint(actor, json)).children.worker.snapshot
            .status
        ).toBe('active');
      })
    );
  });

  it('captures a future timer and restores its remaining delay', async () => {
    const requested: number[] = [];
    let pending = 0;
    const clock = Layer.effect(
      Clock.Clock,
      Effect.map(TestClock.make(), (clock) => ({
        ...clock,
        sleep: (duration: Duration.Duration) =>
          Effect.suspend(() => {
            requested.push(Duration.toMillis(duration));
            pending++;
            return clock.sleep(duration);
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                pending--;
              })
            )
          )
      }))
    );
    const untilPending = Effect.promise(async () => {
      const deadline = Date.now() + 1000;
      while (pending !== 1) {
        if (Date.now() > deadline)
          throw new Error('Timed out waiting for timer');
        await new Promise((resolve) => setTimeout(resolve, 1));
      }
    });
    const machine = createMachine({
      initial: 'waiting',
      states: { waiting: { after: { 1000: { target: 'done' } } }, done: {} }
    });
    await Effect.runPromise(
      Effect.gen(function* () {
        const saved = yield* Effect.scoped(
          Effect.gen(function* () {
            const actor = yield* createEffectActor(machine);
            yield* untilPending;
            yield* TestClock.adjust('600 millis');
            const snapshot = JSON.parse(yield* checkpoint(actor, json));
            expect(snapshot.value).toBe('waiting');
            return snapshot;
          })
        );
        yield* Effect.scoped(
          Effect.gen(function* () {
            const actor = yield* createEffectActor(machine, {
              snapshot: saved
            });
            yield* untilPending;
            expect(requested).toEqual([1000, 400]);
            yield* TestClock.adjust('400 millis');
            expect(JSON.parse(yield* checkpoint(actor, json)).value).toBe(
              'done'
            );
          })
        );
      }).pipe(Effect.provide(clock))
    );
  });

  it('captures initially final and queued final states', async () => {
    const initial = createMachine({
      initial: 'done',
      states: { done: { type: 'final' } }
    });
    const terminal = createMachine({
      initial: 'active',
      states: {
        active: { on: { FINISH: { target: 'done' } } },
        done: { type: 'final' }
      }
    });
    await run(
      Effect.gen(function* () {
        const first = yield* createEffectActor(initial);
        expect(JSON.parse(yield* checkpoint(first, json)).status).toBe('done');
        const second = yield* createEffectActor(terminal);
        second.send({ type: 'FINISH' });
        expect(JSON.parse(yield* checkpoint(second, json)).status).toBe('done');
      })
    );
  });

  it('captures final state after effects when requested by a done subscriber', async () => {
    let marks = 0;
    const machine = setupEffect({
      actors: { child },
      actions: {
        mark: (_args: unknown) => {
          marks++;
          return Effect.void;
        }
      }
    }).createMachine({
      initial: 'active',
      states: {
        active: {
          invoke: { id: 'child', src: 'child' },
          on: { FINISH: { target: 'done' } }
        },
        done: {
          type: 'final',
          entry: (args, enq) => enq(args.actions.mark, args)
        }
      }
    });
    await run(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(machine);
        let capture:
          | Fiber.Fiber<string, ActorStoppedError | Cause.UnknownError>
          | undefined;
        actor.subscribe((snapshot) => {
          if (snapshot.status === 'done') {
            expect(marks).toBe(0);
            capture = Effect.runFork(checkpoint(actor, () => String(marks)));
          }
        });
        yield* sendAndAwait(actor, { type: 'FINISH' });
        expect(capture).toBeDefined();
        expect(yield* Fiber.join(capture!)).toBe('1');
      })
    );
  });

  it('rejects a pending request when the actor stops', async () => {
    await run(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(parent);
        let calls = 0;
        const pending = yield* Effect.forkChild(
          checkpoint(actor, () => {
            calls++;
            return 'saved';
          }),
          { startImmediately: true }
        );
        actor.stop();
        expect(yield* Effect.flip(Fiber.join(pending))).toBeInstanceOf(
          ActorStoppedError
        );
        expect(calls).toBe(0);
      })
    );
  });

  it('types persisted context and requires synchronous string encoders', async () => {
    await run(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(parent);
        checkpoint(actor, (snapshot) => {
          expectTypeOf(snapshot.context.reports).toEqualTypeOf<number>();
          // @ts-expect-error persisted context has no missing property
          snapshot.context.missing;
          return JSON.stringify(snapshot);
        });
        checkpoint<typeof parent>((snapshot) => {
          expectTypeOf(snapshot.context.extra).toEqualTypeOf<number>();
          return JSON.stringify(snapshot);
        })(actor);
        // @ts-expect-error the encoder must return a string
        checkpoint(actor, (snapshot) => snapshot);
        // @ts-expect-error asynchronous encoders are not supported
        checkpoint(actor, async () => 'saved');
        // @ts-expect-error the curried encoder must return a string
        checkpoint<typeof parent>(() => 42);
      })
    );
  });

  it('fails if the actor errors before capture', async () => {
    const machine = createMachine({
      on: {
        FAIL: () => {
          throw new Error('failure');
        }
      }
    });
    await run(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(machine);
        actor.send({ type: 'FAIL' });
        expect(yield* Effect.flip(checkpoint(actor, json))).toBeInstanceOf(
          ActorStoppedError
        );
      })
    );
  });
});
