import { describe, expect, it } from 'vitest';
import { createActor, setup, types } from '../src/index.ts';
import { restoreActor } from '../src/persist/index.ts';
import { withExecutionIdentity } from '../src/system.ts';

const copy = <T>(value: T): T => JSON.parse(JSON.stringify(value));

const append = setup({
  schemas: { events: { APPEND: types<{ value: string }>() } }
}).createMachine({
  context: { text: '' },
  on: {
    APPEND: ({ context, event }) => ({
      context: { text: context.text + event.value }
    })
  }
});

describe('native actor pre-start recovery', () => {
  it('preserves a saved mailbox across repeated unstarted checkpoints', () => {
    const original = createActor(append);
    original.send({ type: 'APPEND', value: 'A' });
    const first = restoreActor(append, copy(original.getPersistedSnapshot()));
    const second = restoreActor(append, copy(first.getPersistedSnapshot()));
    const third = restoreActor(
      append,
      copy(second.getPersistedSnapshot())
    ).start();
    expect(third.getSnapshot().context.text).toBe('A');
  });

  it('processes restored input before a new pre-start send, including after another checkpoint', () => {
    const original = createActor(append);
    original.send({ type: 'APPEND', value: 'A' });
    const restored = restoreActor(
      append,
      copy(original.getPersistedSnapshot())
    );
    restored.send({ type: 'APPEND', value: 'B' });
    const resumed = restoreActor(
      append,
      copy(restored.getPersistedSnapshot())
    ).start();
    expect(resumed.getSnapshot().context.text).toBe('AB');
  });

  it('processes restored input before a new pre-start send without another checkpoint', () => {
    const original = createActor(append);
    original.send({ type: 'APPEND', value: 'A' });
    const restored = restoreActor(
      append,
      copy(original.getPersistedSnapshot())
    );
    restored.send({ type: 'APPEND', value: 'B' });
    restored.start();
    expect(restored.getSnapshot().context.text).toBe('AB');
  });

  it('preserves an unstarted child mailbox and its FIFO prefix across checkpoints', () => {
    const machine = setup({ actors: { append } }).createMachine({
      invoke: { id: 'child', src: 'append' }
    });
    const original = createActor(machine);
    original.getSnapshot().children.child!.send({ type: 'APPEND', value: 'A' });
    const first = restoreActor(machine, copy(original.getPersistedSnapshot()));
    first.getSnapshot().children.child!.send({ type: 'APPEND', value: 'B' });
    const second = restoreActor(machine, copy(first.getPersistedSnapshot()));
    const resumed = restoreActor(
      machine,
      copy(second.getPersistedSnapshot())
    ).start();
    expect(
      resumed.getSnapshot().children.child!.getSnapshot().context.text
    ).toBe('AB');
  });

  it('reports malformed mailbox data as an actor error without throwing from start', () => {
    const original = createActor(append);
    original.send({ type: 'APPEND', value: 'A' });
    const saved = copy(original.getPersistedSnapshot()) as ReturnType<
      typeof original.getPersistedSnapshot
    > & {
      _xstateRecovery: { type: string; version: number; mailbox: unknown[] };
    };
    saved._xstateRecovery.mailbox = [null];
    const restored = restoreActor(append, saved);
    restored.subscribe({ error: () => {} });
    expect(() => restored.start()).not.toThrow();
    expect(restored.getSnapshot().status).toBe('error');
  });

  it('reports a missing pending action source as an actor error without throwing from start', () => {
    const machine = setup({ actions: { record: () => {} } }).createMachine({
      entry: ({ actions }, enq) => enq(actions.record)
    });
    const original = createActor(machine);
    const saved = copy(original.getPersistedSnapshot()) as ReturnType<
      typeof original.getPersistedSnapshot
    > & { _xstateRecovery: { effects: Array<{ source?: string }> } };
    saved._xstateRecovery.effects[0].source = 'missing';
    const restored = restoreActor(machine, saved);
    restored.subscribe({ error: () => {} });
    expect(() => restored.start()).not.toThrow();
    expect(restored.getSnapshot().status).toBe('error');
  });
  it('keeps a stale completion stale when restored session numbering reuses its old id', () => {
    const child = setup({}).createMachine({});
    let capture: (() => void) | undefined;
    let saved: unknown;
    const machine = setup({ actors: { child } }).createMachine({
      initial: 'working',
      states: {
        working: {
          invoke: { id: 'w', src: 'child', onDone: { target: 'finished' } },
          on: {
            REPLACE: { target: 'working', reenter: true },
            CAPTURE: () => capture?.()
          }
        },
        finished: { type: 'final' }
      }
    });
    const actor = withExecutionIdentity(
      { executionId: 'stable', nextSessionId: 0 },
      () => createActor(machine)
    ).start();
    const previous = actor.getSnapshot().children.w!;
    actor.send({ type: 'REPLACE' });
    const current = actor.getSnapshot().children.w!;
    expect(previous.sessionId).not.toBe(current.sessionId);
    capture = () => {
      capture = undefined;
      actor._send(
        {
          type: 'xstate.done.actor',
          actorId: 'w',
          output: undefined,
          sessionId: previous.sessionId
        } as never,
        previous
      );
      saved = copy(actor.getPersistedSnapshot());
    };
    actor.send({ type: 'CAPTURE' });
    const restored = withExecutionIdentity(
      { executionId: 'stable', nextSessionId: 0 },
      () =>
        restoreActor(
          machine,
          saved as ReturnType<typeof actor.getPersistedSnapshot>
        )
    );
    expect(restored.getSnapshot().children.w!.sessionId).toBe(
      previous.sessionId
    );
    restored.start();
    expect(restored.getSnapshot().value).toBe('working');
  });
});
