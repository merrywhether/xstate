import { expect, it } from 'vitest';
import { restoreActor } from '../src/persist/index.ts';
import {
  createActor,
  createMachine,
  createStepLogic,
  setup
} from '../src/index.ts';

it('rejects a pending keyed step effect created by an ordinary event', () => {
  let capture: (() => void) | undefined;
  let failure: unknown;
  let calls = 0;
  const child = createStepLogic({
    context: {},
    run: ({ event }, enq) => {
      if (event.type === 'GO') {
        enq.sendBack({ type: 'REPORT' });
        enq.effect('later', () => {
          calls++;
        });
      }
    }
  });
  const machine = setup({ actors: { child } }).createMachine({
    invoke: { id: 'child', src: 'child' },
    on: { REPORT: () => capture?.() }
  });
  const actor = createActor(machine).start();
  capture = () => {
    capture = undefined;
    try {
      actor.getPersistedSnapshot();
    } catch (error) {
      failure = error;
    }
  };
  actor.getSnapshot().children.child!.send({ type: 'GO' });
  expect(failure).toBeInstanceOf(Error);
  expect((failure as Error).message).toContain(
    'Unable to persist pending effect'
  );
  expect(calls).toBe(1);
});

it('restores an initial keyed step attachment through normal initialization', () => {
  let calls = 0;
  const logic = createStepLogic({
    context: {},
    run: ({ event }, enq) => {
      if (event.type === 'xstate.init')
        enq.effect('initial', () => {
          calls++;
        });
    }
  });
  const actor = createActor(logic);
  const saved = JSON.parse(JSON.stringify(actor.getPersistedSnapshot()));
  const restored = restoreActor(logic, saved).start();
  expect(restored.getSnapshot().status).toBe('active');
  expect(calls).toBe(1);
});
