import { describe, expect, it } from 'vitest';
import { createActor, setup, types } from '../src/index.ts';
import { restoreActor } from '../src/persist/index.ts';

const copy = <T>(value: T): T => JSON.parse(JSON.stringify(value));

describe('persisted queued values', () => {
  const receiver = (receive: (value: unknown) => void) =>
    setup({
      schemas: { events: { READ: types<{ value: unknown }>() } }
    }).createMachine({
      on: {
        READ: ({ event }) => {
          receive(event.value);
          return {};
        }
      }
    });

  it('preserves an unread native error stack, cause and prototype', () => {
    let received: unknown;
    const machine = receiver((value) => {
      received = value;
    });
    const actor = createActor(machine);
    actor.send({
      type: 'READ',
      value: new TypeError('broken', { cause: new Error('cause') })
    });
    restoreActor(machine, copy(actor.getPersistedSnapshot())).start();
    expect(received).toBeInstanceOf(TypeError);
    if (!(received instanceof TypeError))
      throw new Error('Error was not restored.');
    expect(received.message).toBe('broken');
    expect(received.stack).toContain('TypeError: broken');
    expect(received.cause).toBeInstanceOf(Error);
    if (!(received.cause instanceof Error))
      throw new Error('Cause was not restored.');
    expect(received.cause.message).toBe('cause');
  });

  it('rejects custom stack accessors without calling them', () => {
    const actor = createActor(receiver(() => {}));
    const error = new Error('broken');
    let called = false;
    Object.defineProperty(error, 'stack', {
      get() {
        called = true;
        return 'fake';
      }
    });
    actor.send({ type: 'READ', value: error });
    expect(() => actor.getPersistedSnapshot()).toThrow(
      'Unable to persist error accessor.'
    );
    expect(called).toBe(false);
  });

  it('preserves shared references and null prototypes', () => {
    let received: unknown;
    const machine = receiver((value) => {
      received = value;
    });
    const actor = createActor(machine);
    const shared: Record<string, unknown> = Object.assign(Object.create(null), {
      value: 1
    });
    actor.send({ type: 'READ', value: [shared, shared] });
    restoreActor(machine, copy(actor.getPersistedSnapshot())).start();
    expect(Array.isArray(received)).toBe(true);
    if (!Array.isArray(received)) throw new Error('Array was not restored.');
    expect(received[0]).toBe(received[1]);
    expect(Object.getPrototypeOf(received[0])).toBeNull();
    expect(received[0].value).toBe(1);
  });
});
