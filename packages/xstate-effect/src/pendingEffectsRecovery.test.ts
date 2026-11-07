import { Clock, Duration, Effect, Layer, References, Schema } from 'effect';
import { TestClock } from 'effect/testing';
import { createMachine, setup, types } from 'xstate';
import type { AnyActor, AnyActorRef } from 'xstate';

import { describe, it, expect } from 'vitest';
import { createEffectActor, setupEffect, waitFor } from './index.ts';
import type { EffectActionArgs } from './index.ts';
import { sendAndAwait } from './test.ts';

const copy = <T>(x: T): T => JSON.parse(JSON.stringify(x)) as T;
const required = <T>(x: T | undefined): T => {
  if (x === undefined) throw new Error('snapshot was not captured');
  return x;
};

describe('recovery windows', () => {
  it('restores a child send pending when the parent publishes its transition', async () => {
    const child = createMachine({
      context: { n: 0 },
      on: { INC: ({ context }) => ({ context: { n: context.n + 1 } }) }
    });
    const machine = setup({
      actors: { child },
      schemas: { events: { GO: types<{}>(), PING: types<{}>() } }
    }).createMachine({
      invoke: { src: 'child', id: 'child' },
      context: { sent: false },
      on: {
        PING: {},
        GO: ({ children }, enq) => {
          enq.sendTo(children.child!, { type: 'INC' });
          return { context: { sent: true } };
        }
      }
    });
    let saved: ReturnType<typeof machine.getPersistedSnapshot> | undefined;
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const actor = yield* createEffectActor(machine);
          yield* sendAndAwait(actor, { type: 'PING' });
          actor.subscribe((s) => {
            if (s.context.sent && !saved)
              saved = copy(actor.getPersistedSnapshot());
          });
          yield* sendAndAwait(actor, { type: 'GO' });
          expect(
            actor.getSnapshot().children.child!.getSnapshot().context.n
          ).toBe(1);
        })
      )
    );
    expect(required(saved)).toMatchObject({
      context: { sent: true },
      children: { child: { snapshot: { context: { n: 0 } } } }
    });
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const actor = yield* createEffectActor(machine, {
            snapshot: required(saved)
          });
          yield* sendAndAwait(actor, { type: 'PING' });
          expect(
            actor.getSnapshot().children.child!.getSnapshot().context.n
          ).toBe(1);
        })
      )
    );
  });
  it('restores an accepted event that has not been processed', async () => {
    const machine = createMachine({
      context: { n: 0 },
      on: {
        PING: {},
        INC: ({ context }) => ({ context: { n: context.n + 1 } })
      }
    });
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const actor = yield* createEffectActor(machine);
          yield* sendAndAwait(actor, { type: 'PING' });
          actor.send({ type: 'INC' });
          const saved = copy(actor.getPersistedSnapshot());
          expect(saved).toMatchObject({ context: { n: 0 } });
          yield* sendAndAwait(actor, { type: 'PING' });
          expect(actor.getSnapshot().context.n).toBe(1);
          const restored = yield* createEffectActor(machine, {
            snapshot: required(saved)
          });
          yield* sendAndAwait(restored, { type: 'PING' });
          expect(restored.getSnapshot().context.n).toBe(1);
        })
      )
    );
  });
  it.each([5, 2048])(
    'resumes a partially executed batch with %i operations per yield',
    async (maxOps) => {
      const child = createMachine({
        context: { n: 0 },
        on: { INC: ({ context }) => ({ context: { n: context.n + 1 } }) }
      });
      const machine = setup({
        actors: { child },
        schemas: { events: { GO: types<{}>(), PING: types<{}>() } }
      }).createMachine({
        invoke: { src: 'child', id: 'child' },
        context: { sent: false },
        on: {
          PING: {},
          GO: ({ children }, enq) => {
            enq.sendTo(children.child!, { type: 'INC' });
            enq.sendTo(children.child!, { type: 'INC' });
            return { context: { sent: true } };
          }
        }
      });
      let saved: ReturnType<typeof machine.getPersistedSnapshot> | undefined;
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const actor = yield* createEffectActor(machine);
            yield* sendAndAwait(actor, { type: 'PING' });
            actor.getSnapshot().children.child!.subscribe((s) => {
              if (s.context.n === 1) saved = copy(actor.getPersistedSnapshot());
            });
            yield* sendAndAwait(actor, { type: 'GO' });
            expect(
              actor.getSnapshot().children.child!.getSnapshot().context.n
            ).toBe(2);
          })
        ).pipe(Effect.provideService(References.MaxOpsBeforeYield, maxOps))
      );
      expect(required(saved)).toMatchObject({
        children: { child: { snapshot: { context: { n: 1 } } } }
      });
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const actor = yield* createEffectActor(machine, {
              snapshot: required(saved)
            });
            yield* sendAndAwait(actor, { type: 'PING' });
            expect(
              actor.getSnapshot().children.child!.getSnapshot().context.n
            ).toBe(2);
          })
        ).pipe(Effect.provideService(References.MaxOpsBeforeYield, maxOps))
      );
    }
  );
  it('persists unfinished effects again before a restored batch resumes', async () => {
    const child = createMachine({
      context: { count: 0 },
      on: { INC: ({ context }) => ({ context: { count: context.count + 1 } }) }
    });
    const machine = setup({ actors: { child } }).createMachine({
      invoke: { src: 'child', id: 'child' },
      on: {
        PING: {},
        GO: ({ children }, enq) => {
          enq.sendTo(children.child!, { type: 'INC' });
          enq.sendTo(children.child!, { type: 'INC' });
        }
      }
    });
    let first: ReturnType<typeof machine.getPersistedSnapshot> | undefined;
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const original = yield* createEffectActor(machine);
          yield* sendAndAwait(original, { type: 'PING' });
          original.getSnapshot().children.child!.subscribe((snapshot) => {
            if (snapshot.context.count === 1 && !first) {
              first = copy(original.getPersistedSnapshot());
            }
          });
          yield* sendAndAwait(original, { type: 'GO' });
          expect(
            original.getSnapshot().children.child!.getSnapshot().context.count
          ).toBe(2);
          const restored = yield* createEffectActor(machine, {
            snapshot: required(first)
          });
          const second = copy(restored.getPersistedSnapshot());
          expect(second).toMatchObject({
            children: { child: { snapshot: { context: { count: 1 } } } }
          });
          yield* sendAndAwait(restored, { type: 'PING' });
          expect(
            restored.getSnapshot().children.child!.getSnapshot().context.count
          ).toBe(2);
          const restoredAgain = yield* createEffectActor(machine, {
            snapshot: second
          });
          yield* sendAndAwait(restoredAgain, { type: 'PING' });
          expect(
            restoredAgain.getSnapshot().children.child!.getSnapshot().context
              .count
          ).toBe(2);
        })
      )
    );
  });

  it('delivers child completion captured before its done event reaches the parent', async () => {
    const child = createMachine({
      initial: 'working',
      states: {
        working: { on: { FINISH: { target: 'done' } } },
        done: { type: 'final' }
      }
    });
    const machine = setup({
      actors: { child },
      schemas: { events: { PING: types<{}>() } }
    }).createMachine({
      on: { PING: {} },
      initial: 'active',
      states: {
        active: {
          invoke: { src: 'child', id: 'child', onDone: { target: 'done' } }
        },
        done: { type: 'final' }
      }
    });
    let saved: ReturnType<typeof machine.getPersistedSnapshot> | undefined;
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const actor = yield* createEffectActor(machine);
          yield* sendAndAwait(actor, { type: 'PING' });
          actor.getSnapshot().children.child!.subscribe({
            complete: () => {
              saved = copy(actor.getPersistedSnapshot());
            }
          });
          actor.getSnapshot().children.child!.send({ type: 'FINISH' });
          yield* waitFor(actor, (s) => s.status === 'done');
          expect(actor.getSnapshot().status).toBe('done');
        })
      )
    );
    expect(required(saved)).toMatchObject({
      value: 'active',
      children: { child: { snapshot: { status: 'done' } } }
    });
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const actor = yield* createEffectActor(machine, {
            snapshot: required(saved)
          });
          yield* waitFor(actor, (s) => s.status === 'done', {
            timeout: Duration.seconds(1)
          });
          expect(actor.getSnapshot().status).toBe('done');
        })
      )
    );
  });

  it('resumes a mixed batch in order without repeating the accepted send', async () => {
    const child = createMachine({
      context: { count: 0 },
      on: { INC: ({ context }) => ({ context: { count: context.count + 1 } }) }
    });
    const actions: string[] = [];
    const machine = setup({
      actors: { child },
      actions: {
        record: () => {
          actions.push('record');
        }
      },
      schemas: { events: { GO: types<{}>(), PING: types<{}>() } }
    }).createMachine({
      invoke: { src: 'child', id: 'child' },
      on: {
        PING: {},
        GO: (args, enq) => {
          enq.sendTo(args.children.child!, { type: 'INC' });
          enq(args.actions.record);
          enq.sendTo(args.children.child!, { type: 'INC' });
        }
      }
    });
    let saved: ReturnType<typeof machine.getPersistedSnapshot> | undefined;
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const original = yield* createEffectActor(machine);
          yield* sendAndAwait(original, { type: 'PING' });
          original.getSnapshot().children.child!.subscribe((snapshot) => {
            if (snapshot.context.count === 1 && !saved) {
              saved = copy(original.getPersistedSnapshot());
            }
          });
          yield* sendAndAwait(original, { type: 'GO' });
          expect(
            original.getSnapshot().children.child!.getSnapshot().context.count
          ).toBe(2);
          expect(actions).toEqual(['record']);
          const restored = yield* createEffectActor(machine, {
            snapshot: required(saved)
          });
          const restoredOrder: string[] = [];
          restored.getSnapshot().children.child!.subscribe((snapshot) => {
            if (snapshot.context.count === 2)
              restoredOrder.push(`send after ${actions.length} records`);
          });
          yield* sendAndAwait(restored, { type: 'PING' });
          expect(actions).toEqual(['record', 'record']);
          expect(restoredOrder).toEqual(['send after 2 records']);
          expect(
            restored.getSnapshot().children.child!.getSnapshot().context.count
          ).toBe(2);
        })
      )
    );
  });

  it('restores nested child effects and the remainder of the parent batch', async () => {
    const child = createMachine({
      context: { count: 0 },
      on: {
        INC: ({ context, parent }, enq) => {
          enq.sendTo(parent!, { type: 'REPORT' });
          return { context: { count: context.count + 1 } };
        }
      }
    });
    const machine = setup({
      actors: { child },
      schemas: {
        events: { GO: types<{}>(), PING: types<{}>(), REPORT: types<{}>() }
      }
    }).createMachine({
      context: { reports: 0 },
      invoke: { src: 'child', id: 'child' },
      on: {
        PING: {},
        GO: ({ children }, enq) => {
          enq.sendTo(children.child!, { type: 'INC' });
          enq.sendTo(children.child!, { type: 'INC' });
        },
        REPORT: ({ context }) => ({ context: { reports: context.reports + 1 } })
      }
    });
    let saved: ReturnType<typeof machine.getPersistedSnapshot> | undefined;
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const original = yield* createEffectActor(machine);
          yield* sendAndAwait(original, { type: 'PING' });
          original.getSnapshot().children.child!.subscribe((snapshot) => {
            if (snapshot.context.count === 1 && !saved) {
              saved = copy(original.getPersistedSnapshot());
            }
          });
          yield* sendAndAwait(original, { type: 'GO' });
          yield* waitFor(original, (s) => s.context.reports === 2, {
            timeout: Duration.seconds(1)
          });
          const restored = yield* createEffectActor(machine, {
            snapshot: required(saved)
          });
          yield* waitFor(restored, (s) => s.context.reports === 2, {
            timeout: Duration.seconds(1)
          });
          expect(
            restored.getSnapshot().children.child!.getSnapshot().context.count
          ).toBe(2);
          expect(restored.getSnapshot().context.reports).toBe(2);
        })
      )
    );
  });

  it('restores an effect pending on the transition to a final state', async () => {
    let calls = 0;
    let notify = () => {};
    const machine = setup({
      actions: {
        record: () => {
          calls++;
          notify();
        }
      },
      schemas: { events: { GO: types<{}>() } }
    }).createMachine({
      initial: 'active',
      states: {
        active: {
          on: {
            GO: (args, enq) => {
              enq(args.actions.record);
              return { target: 'done' };
            }
          }
        },
        done: { type: 'final' }
      }
    });
    let saved: ReturnType<typeof machine.getPersistedSnapshot> | undefined;
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const original = yield* createEffectActor(machine);
          original.subscribe((snapshot) => {
            if (snapshot.status === 'done' && !saved) {
              saved = copy(original.getPersistedSnapshot());
            }
          });
          yield* sendAndAwait(original, { type: 'GO' });
          expect(calls).toBe(1);
          const completed = new Promise<void>((resolve) => {
            notify = resolve;
          });
          yield* createEffectActor(machine, { snapshot: required(saved) });
          yield* Effect.promise(() => completed);
          expect(calls).toBe(2);
        })
      )
    );
  });

  it('restores an initial send captured before it executes', async () => {
    const child = createMachine({
      context: { count: 0 },
      on: { INC: ({ context }) => ({ context: { count: context.count + 1 } }) }
    });
    const machine = setup({ actors: { child } }).createMachine({
      invoke: { src: 'child', id: 'child' },
      entry: ({ children }, enq) =>
        enq.sendTo(children.child!, { type: 'INC' }),
      on: { PING: {} }
    });
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const original = yield* createEffectActor(machine);
          const saved = copy(original.getPersistedSnapshot());
          expect(saved).toMatchObject({
            children: { child: { snapshot: { context: { count: 0 } } } }
          });
          yield* sendAndAwait(original, { type: 'PING' });
          expect(
            original.getSnapshot().children.child!.getSnapshot().context.count
          ).toBe(1);
          const restored = yield* createEffectActor(machine, {
            snapshot: saved
          });
          yield* sendAndAwait(restored, { type: 'PING' });
          expect(
            restored.getSnapshot().children.child!.getSnapshot().context.count
          ).toBe(1);
        })
      )
    );
  });
});

it('rebinds registered action arguments to the restored actor tree', async () => {
  const child = createMachine({
    context: { count: 0 },
    on: { INC: ({ context }) => ({ context: { count: context.count + 1 } }) }
  });
  const seen: Array<{
    value: number;
    event: string;
    self: string;
    child: string;
  }> = [];
  const machine = setup({
    actors: { child },
    actions: {
      record: (args: {
        value: number;
        event: string;
        self: AnyActor;
        child: AnyActor;
      }) => {
        seen.push({
          value: args.value,
          event: args.event,
          self: args.self.sessionId!,
          child: args.child.sessionId!
        });
        args.self.send({ type: 'REPORT' });
        args.child.send({ type: 'INC' });
      }
    }
  }).createMachine({
    context: { value: 0, reports: 0 },
    invoke: { src: 'child', id: 'child' },
    on: {
      PING: {},
      REPORT: ({ context }) => ({
        context: { ...context, reports: context.reports + 1 }
      }),
      GO: (args, enq) => {
        enq(args.actions.record, {
          value: 7,
          event: args.event.type,
          self: args.self,
          child: args.children.child!
        });
        return { context: { ...args.context, value: 7 } };
      }
    }
  });
  let saved: ReturnType<typeof machine.getPersistedSnapshot> | undefined;
  let captureError: unknown;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const original = yield* createEffectActor(machine);
        yield* sendAndAwait(original, { type: 'PING' });
        original.subscribe((snapshot) => {
          if (snapshot.context.value === 7 && !saved && !captureError) {
            try {
              saved = copy(original.getPersistedSnapshot());
            } catch (error) {
              captureError = error;
            }
          }
        });
        yield* sendAndAwait(original, { type: 'GO' });
        expect(captureError).toBeUndefined();
        yield* waitFor(original, (s) => s.context.reports === 1, {
          timeout: Duration.seconds(1)
        });
        const restored = yield* createEffectActor(machine, {
          snapshot: required(saved)
        });
        yield* waitFor(restored, (s) => s.context.reports === 1, {
          timeout: Duration.seconds(1)
        });
        expect(
          restored.getSnapshot().children.child!.getSnapshot().context.count
        ).toBe(1);
        expect(seen).toHaveLength(2);
        expect(seen.map(({ value, event }) => ({ value, event }))).toEqual([
          { value: 7, event: 'GO' },
          { value: 7, event: 'GO' }
        ]);
        expect(seen[1].self).not.toBe(seen[0].self);
        expect(seen[1].child).not.toBe(seen[0].child);
        expect(original.getSnapshot().context.reports).toBe(1);
        expect(
          original.getSnapshot().children.child!.getSnapshot().context.count
        ).toBe(1);
      })
    )
  );
});

it('rejects a snapshot with an unsupported pending inline action', async () => {
  const machine = createMachine({
    context: { started: false },
    on: {
      GO: ({ context }, enq) => {
        enq(() => {});
        return { context: { ...context, started: true } };
      }
    }
  });
  let captureError: unknown;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const actor = yield* createEffectActor(machine);
        actor.subscribe((snapshot) => {
          if (snapshot.context.started) {
            try {
              actor.getPersistedSnapshot();
            } catch (error) {
              captureError = error;
            }
          }
        });
        yield* sendAndAwait(actor, { type: 'GO' });
        expect(captureError).toBeInstanceOf(Error);
        expect((captureError as Error).message).toMatch(
          /persist pending effect/i
        );
      })
    )
  );
});

it('restores full setupEffect transition arguments in the restored child tree', async () => {
  let capture: (() => void) | undefined;
  const grandchild = createMachine({
    context: { count: 0 },
    on: {
      INC: ({ context }) => ({ context: { count: context.count + 1 } })
    }
  });
  const seen: Array<{
    value: number;
    event: string;
    self: AnyActorRef;
    parent: AnyActorRef;
    child: AnyActorRef;
    source: boolean;
  }> = [];
  const child = setupEffect({
    schemas: { context: Schema.Struct({ value: Schema.Number }) },
    actors: { grandchild },
    actions: {
      checkpoint: (_args: EffectActionArgs<{ value: number }>) => {
        capture?.();
        return Effect.void;
      },
      record: (args: EffectActionArgs<{ value: number }>) =>
        Effect.sync(() => {
          seen.push({
            value: args.context.value,
            event: args.event.type,
            self: args.self,
            parent: args.parent!,
            child: args.children.grandchild!,
            source: typeof args.actions.record === 'function'
          });
          args.parent!.send({ type: 'REPORT' });
          args.children.grandchild!.send({ type: 'INC' });
        })
    }
  }).createMachine({
    context: { value: 3 },
    invoke: { src: 'grandchild', id: 'grandchild' },
    on: {
      WORK: (args, enq) => {
        enq(args.actions.checkpoint, args);
        enq(args.actions.record, args);
        return { context: { value: 4 } };
      }
    }
  });
  const machine = setup({ actors: { child } }).createMachine({
    context: { reports: 0 },
    invoke: { src: 'child', id: 'child' },
    on: {
      PING: {},
      GO: ({ children }, enq) => enq.sendTo(children.child!, { type: 'WORK' }),
      REPORT: ({ context }) => ({ context: { reports: context.reports + 1 } })
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
          capture = undefined;
          try {
            saved = copy(original.getPersistedSnapshot());
          } catch (error) {
            captureError = error;
          }
        };
        yield* sendAndAwait(original, { type: 'GO' });
        expect(captureError).toBeUndefined();
        yield* waitFor(original, (s) => s.context.reports === 1, {
          timeout: Duration.seconds(1)
        });
        const restored = yield* createEffectActor(machine, {
          snapshot: required(saved)
        });
        yield* waitFor(restored, (s) => s.context.reports === 1, {
          timeout: Duration.seconds(1)
        });
        expect(seen).toHaveLength(2);
        expect(
          seen.map(({ value, event, source }) => ({ value, event, source }))
        ).toEqual([
          { value: 3, event: 'WORK', source: true },
          { value: 3, event: 'WORK', source: true }
        ]);
        expect(seen[1].self).not.toBe(seen[0].self);
        expect(seen[1].parent).not.toBe(seen[0].parent);
        expect(seen[1].child).not.toBe(seen[0].child);
        expect(
          restored
            .getSnapshot()
            .children.child!.getSnapshot()
            .children.grandchild!.getSnapshot().context.count
        ).toBe(1);
        expect(original.getSnapshot().context.reports).toBe(1);
      })
    )
  );
});

it('starts a pending child and runs its initial work after restore', async () => {
  const child = createMachine({
    entry: ({ parent }, enq) => enq.sendTo(parent!, { type: 'REPORT' })
  });
  const machine = setup({ actors: { child } }).createMachine({
    context: { spawned: false, reports: 0 },
    on: {
      PING: {},
      GO: ({ context, actors }, enq) => {
        enq.spawn(actors.child, { id: 'child' });
        return { context: { ...context, spawned: true } };
      },
      REPORT: ({ context }) => ({
        context: { ...context, reports: context.reports + 1 }
      })
    }
  });
  let saved: ReturnType<typeof machine.getPersistedSnapshot> | undefined;
  let captureError: unknown;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const original = yield* createEffectActor(machine);
        yield* sendAndAwait(original, { type: 'PING' });
        original.subscribe((snapshot) => {
          if (
            snapshot.context.spawned &&
            snapshot.context.reports === 0 &&
            !saved &&
            !captureError
          ) {
            try {
              saved = copy(original.getPersistedSnapshot());
            } catch (error) {
              captureError = error;
            }
          }
        });
        yield* sendAndAwait(original, { type: 'GO' });
        expect(captureError).toBeUndefined();
        yield* waitFor(original, (s) => s.context.reports === 1, {
          timeout: Duration.seconds(1)
        });
        const restored = yield* createEffectActor(machine, {
          snapshot: required(saved)
        });
        yield* waitFor(restored, (s) => s.context.reports === 1, {
          timeout: Duration.seconds(1)
        });
        expect(restored.getSnapshot().context.reports).toBe(1);
      })
    )
  );
});

it('schedules an unaccepted timer once with its full delay after restore', async () => {
  const requested: number[] = [];
  const observers: Array<() => void> = [];
  const layer = Layer.effect(
    Clock.Clock,
    Effect.map(TestClock.make(), (clock) => ({
      ...clock,
      sleep: (duration: Duration.Duration) =>
        Effect.suspend(() => {
          requested.push(Duration.toMillis(duration));
          observers.shift()?.();
          return clock.sleep(duration);
        })
    }))
  );
  const nextSleep = () =>
    new Promise<void>((resolve) => observers.push(resolve));
  const machine = createMachine({
    context: { scheduled: false, fired: 0 },
    on: {
      PING: {},
      GO: ({ context }, enq) => {
        enq.raise({ type: 'TICK' }, { delay: 1000, id: 'timer' });
        return { context: { ...context, scheduled: true } };
      },
      TICK: ({ context }) => ({
        context: { ...context, fired: context.fired + 1 }
      })
    }
  });
  let saved: ReturnType<typeof machine.getPersistedSnapshot> | undefined;
  let captureError: unknown;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const original = yield* createEffectActor(machine);
        yield* sendAndAwait(original, { type: 'PING' });
        original.subscribe((snapshot) => {
          if (snapshot.context.scheduled && !saved && !captureError) {
            try {
              saved = copy(original.getPersistedSnapshot());
            } catch (error) {
              captureError = error;
            }
          }
        });
        const firstSleep = nextSleep();
        yield* sendAndAwait(original, { type: 'GO' });
        yield* Effect.promise(() => firstSleep);
        expect(captureError).toBeUndefined();
        yield* TestClock.adjust('600 millis');
        const secondSleep = nextSleep();
        const restored = yield* createEffectActor(machine, {
          snapshot: required(saved)
        });
        yield* Effect.promise(() => secondSleep);
        expect(requested).toEqual([1000, 1000]);
        yield* TestClock.adjust('400 millis');
        yield* waitFor(original, (s) => s.context.fired === 1, {
          timeout: Duration.seconds(1)
        });
        expect(restored.getSnapshot().context.fired).toBe(0);
        yield* TestClock.adjust('600 millis');
        yield* waitFor(restored, (s) => s.context.fired === 1, {
          timeout: Duration.seconds(1)
        });
      })
    ).pipe(Effect.provide(layer))
  );
});

it('restores a pending timer cancellation before its deadline', async () => {
  let sleeping = () => {};
  const layer = Layer.effect(
    Clock.Clock,
    Effect.map(TestClock.make(), (clock) => ({
      ...clock,
      sleep: (duration: Duration.Duration) =>
        Effect.suspend(() => {
          sleeping();
          return clock.sleep(duration);
        })
    }))
  );
  const machine = createMachine({
    context: { cancelled: false, fired: 0 },
    on: {
      PING: {},
      GO: (_, enq) => enq.raise({ type: 'TICK' }, { delay: 1000, id: 'timer' }),
      CANCEL: ({ context }, enq) => {
        enq.cancel('timer');
        return { context: { ...context, cancelled: true } };
      },
      TICK: ({ context }) => ({
        context: { ...context, fired: context.fired + 1 }
      })
    }
  });
  let saved: ReturnType<typeof machine.getPersistedSnapshot> | undefined;
  let captureError: unknown;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const original = yield* createEffectActor(machine);
        yield* sendAndAwait(original, { type: 'PING' });
        const timerRegistered = new Promise<void>((resolve) => {
          sleeping = resolve;
        });
        yield* sendAndAwait(original, { type: 'GO' });
        yield* Effect.promise(() => timerRegistered);
        original.subscribe((snapshot) => {
          if (snapshot.context.cancelled && !saved && !captureError) {
            try {
              saved = copy(original.getPersistedSnapshot());
            } catch (error) {
              captureError = error;
            }
          }
        });
        yield* sendAndAwait(original, { type: 'CANCEL' });
        expect(captureError).toBeUndefined();
        const restored = yield* createEffectActor(machine, {
          snapshot: required(saved)
        });
        yield* sendAndAwait(restored, { type: 'PING' });
        yield* TestClock.adjust('1 second');
        yield* sendAndAwait(original, { type: 'PING' });
        yield* sendAndAwait(restored, { type: 'PING' });
        expect(original.getSnapshot().context.fired).toBe(0);
        expect(restored.getSnapshot().context.fired).toBe(0);
      })
    ).pipe(Effect.provide(layer))
  );
});

it('restores an armed timer once with its remaining delay after downtime', async () => {
  const requested: number[] = [];
  const observers: Array<() => void> = [];
  const layer = Layer.effect(
    Clock.Clock,
    Effect.map(TestClock.make(), (clock) => ({
      ...clock,
      sleep: (duration: Duration.Duration) =>
        Effect.suspend(() => {
          requested.push(Duration.toMillis(duration));
          observers.shift()?.();
          return clock.sleep(duration);
        })
    }))
  );
  const nextSleep = () =>
    new Promise<void>((resolve) => observers.push(resolve));
  const machine = createMachine({
    context: { fired: 0 },
    on: {
      PING: {},
      GO: (_, enq) => enq.raise({ type: 'TICK' }, { delay: 1000, id: 'timer' }),
      TICK: ({ context }) => ({ context: { fired: context.fired + 1 } })
    }
  });
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const original = yield* createEffectActor(machine);
        yield* sendAndAwait(original, { type: 'PING' });
        const firstSleep = nextSleep();
        yield* sendAndAwait(original, { type: 'GO' });
        yield* Effect.promise(() => firstSleep);
        const saved = copy(original.getPersistedSnapshot());
        yield* TestClock.adjust('600 millis');
        const secondSleep = nextSleep();
        const restored = yield* createEffectActor(machine, { snapshot: saved });
        yield* Effect.promise(() => secondSleep);
        expect(requested).toEqual([1000, 400]);
        yield* TestClock.adjust('400 millis');
        yield* waitFor(original, (s) => s.context.fired === 1, {
          timeout: Duration.seconds(1)
        });
        yield* waitFor(restored, (s) => s.context.fired === 1, {
          timeout: Duration.seconds(1)
        });
        expect(original.getSnapshot().context.fired).toBe(1);
        expect(restored.getSnapshot().context.fired).toBe(1);
      })
    ).pipe(Effect.provide(layer))
  );
});

it('restores full root setupEffect arguments with a rebound self', async () => {
  const seen: Array<{ value: number; event: string; self: AnyActorRef }> = [];
  const machine = setupEffect({
    schemas: { context: Schema.Struct({ value: Schema.Number }) },
    actions: {
      record: (args: EffectActionArgs<{ value: number }>) =>
        Effect.sync(() => {
          seen.push({
            value: args.context.value,
            event: args.event.type,
            self: args.self
          });
          args.self.send({ type: 'REPORT' });
        })
    }
  }).createMachine({
    context: { value: 1 },
    on: {
      PING: {},
      GO: (args, enq) => {
        enq(args.actions.record, args);
        return { context: { value: 2 } };
      },
      REPORT: () => ({ context: { value: 3 } })
    }
  });
  let saved: ReturnType<typeof machine.getPersistedSnapshot> | undefined;
  let captureError: unknown;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const original = yield* createEffectActor(machine);
        yield* sendAndAwait(original, { type: 'PING' });
        original.subscribe((snapshot) => {
          if (snapshot.context.value === 2 && !saved && !captureError) {
            try {
              saved = copy(original.getPersistedSnapshot());
            } catch (error) {
              captureError = error;
            }
          }
        });
        yield* sendAndAwait(original, { type: 'GO' });
        expect(captureError).toBeUndefined();
        yield* waitFor(original, (s) => s.context.value === 3, {
          timeout: Duration.seconds(1)
        });
        const restored = yield* createEffectActor(machine, {
          snapshot: required(saved)
        });
        yield* waitFor(restored, (s) => s.context.value === 3, {
          timeout: Duration.seconds(1)
        });
        expect(seen.map(({ value, event }) => ({ value, event }))).toEqual([
          { value: 1, event: 'GO' },
          { value: 1, event: 'GO' }
        ]);
        expect(seen[1].self).not.toBe(seen[0].self);
        expect(original.getSnapshot().context.value).toBe(3);
      })
    )
  );
});

it('preserves shared action argument identity after restore', async () => {
  const observations: boolean[] = [];
  const machine = setup({
    actions: {
      record: (args: {
        first: { value: number };
        second: { value: number };
      }) => {
        observations.push(args.first === args.second);
      }
    }
  }).createMachine({
    context: { started: false },
    on: {
      PING: {},
      GO: (_, enq) => {
        const shared = { value: 1 };
        enq(_.actions.record, { first: shared, second: shared });
        return { context: { started: true } };
      }
    }
  });
  let saved: ReturnType<typeof machine.getPersistedSnapshot> | undefined;
  let captureError: unknown;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const original = yield* createEffectActor(machine);
        yield* sendAndAwait(original, { type: 'PING' });
        original.subscribe((s) => {
          if (s.context.started && !saved && !captureError) {
            try {
              saved = copy(original.getPersistedSnapshot());
            } catch (error) {
              captureError = error;
            }
          }
        });
        yield* sendAndAwait(original, { type: 'GO' });
        expect(captureError).toBeUndefined();
        const restored = yield* createEffectActor(machine, {
          snapshot: required(saved)
        });
        yield* sendAndAwait(restored, { type: 'PING' });
        expect(observations).toEqual([true, true]);
      })
    )
  );
});

it('restores a child input captured inside its transition handler', async () => {
  let capture = () => {};
  const child = createMachine({
    context: { count: 0 },
    on: {
      INC: ({ context, parent }, enq) => {
        capture();
        enq.sendTo(parent!, { type: 'REPORT' });
        return { context: { count: context.count + 1 } };
      }
    }
  });
  const machine = setup({ actors: { child } }).createMachine({
    context: { reports: 0 },
    invoke: { src: 'child', id: 'child' },
    on: {
      PING: {},
      GO: ({ children }, enq) => enq.sendTo(children.child!, { type: 'INC' }),
      REPORT: ({ context }) => ({ context: { reports: context.reports + 1 } })
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
          if (!saved && !captureError) {
            try {
              saved = copy(original.getPersistedSnapshot());
            } catch (error) {
              captureError = error;
            }
          }
        };
        yield* sendAndAwait(original, { type: 'GO' });
        expect(captureError).toBeUndefined();
        yield* waitFor(original, (s) => s.context.reports === 1, {
          timeout: Duration.seconds(1)
        });
        const restored = yield* createEffectActor(machine, {
          snapshot: required(saved)
        });
        yield* waitFor(restored, (s) => s.context.reports === 1, {
          timeout: Duration.seconds(1)
        });
        expect(
          restored.getSnapshot().children.child!.getSnapshot().context.count
        ).toBe(1);
      })
    )
  );
});
