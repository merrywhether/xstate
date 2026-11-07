import { hasLazyActorCapability } from './actorScope.ts';
import { isRemoteActorRef, isSerializedActorRef } from './remoteActorRef.ts';
import { isRestartedStepEffect } from './actors/logic.ts';
import { cloneMachineSnapshot } from './State.ts';
import { getSnapshotActorRef } from './snapshotActorRef.ts';
import {
  describeExecutableEffect,
  type ExecutableEffectDescriptor
} from './effectDescriptor.ts';
import {
  createCancelEffect,
  createRegisteredActionEffect,
  createStopEffect,
  createTimerEffect,
  createSendToEffect,
  createSpawnEffect,
  createStartEffect,
  createTerminationEffect
} from './transitionActions.ts';
import { readTimerKey } from './timerKey.ts';
import type {
  AnyActor,
  AnyActorScope,
  AnyMachineSnapshot,
  EventObject,
  ExecutableEffect,
  Snapshot,
  SerializedActorRef
} from './types.ts';

// A batch belongs to the actor whose transition produced it. Keep the actual
// resolved operations until execution accepts them; never rerun a transition
// to reconstruct effects.
const pending = new WeakMap<AnyActor, ExecutableEffect[]>();
type Builtin<T extends string> = Extract<
  ExecutableEffectDescriptor,
  { kind: 'builtin'; type: T }
>;
type PendingEventDescriptor =
  | {
      kind: 'builtin';
      type: '@xstate.sendTo';
      source: SerializedActorRef;
      target?: SerializedActorRef;
      id?: string;
      seq?: number;
      delay?: number;
    }
  | {
      kind: 'builtin';
      type: '@xstate.raise';
      source: SerializedActorRef;
      id?: string;
      seq?: number;
      delay?: number;
    }
  | Omit<Builtin<'@xstate.cancel'>, 'event'>;
type PendingDescriptor =
  | {
      kind: 'builtin';
      type: '@xstate.spawn' | '@xstate.start' | '@xstate.terminate';
      actor: SerializedActorRef;
    }
  | {
      kind: 'builtin';
      type: '@xstate.stop';
      source: SerializedActorRef;
      actor: SerializedActorRef;
    }
  | Omit<Builtin<'@xstate.cancel'>, 'event'>
  | {
      kind: 'builtinWithEvent';
      descriptor: PendingEventDescriptor;
      event: Encoded;
    }
  | {
      kind: 'registeredAction';
      source: string;
      args: Encoded;
      params: Encoded;
    };
type Encoded =
  | { tag: 'value'; value: string | number | boolean | null }
  | { tag: 'undefined' }
  | { tag: 'actor'; address: string }
  | { tag: 'system' }
  | { tag: 'source'; group: string; key: string }
  | { tag: 'ref'; id: number }
  | { tag: 'array'; id: number; value: Encoded[] }
  | {
      tag: 'object';
      id: number;
      nullPrototype: boolean;
      value: [string, Encoded][];
    }
  | {
      tag: 'error';
      id: number;
      kind: string;
      properties: [string, Encoded, boolean, boolean, boolean][];
    }
  | {
      tag: 'machineSnapshot';
      id: number;
      address: string;
      nodes: string[];
      history: [string, string[]][];
      data: Encoded;
    };
const restored = new WeakMap<AnyActor, PendingDescriptor[]>();
const owners = new WeakMap<ExecutableEffect, AnyActor>();
let executing: ExecutableEffect | undefined;

type PendingDelivery = {
  effect: ExecutableEffect;
  source: AnyActor | undefined;
  target: AnyActor;
  event: EventObject;
};
const pendingDeliveries = new WeakMap<EventObject, PendingDelivery[]>();
const effectDeliveries = new WeakMap<ExecutableEffect, Set<PendingDelivery>>();

function removePendingDelivery(delivery: PendingDelivery): void {
  const list = pendingDeliveries.get(delivery.event);
  const index = list?.indexOf(delivery) ?? -1;
  if (index !== -1) list!.splice(index, 1);
  if (!list?.length) pendingDeliveries.delete(delivery.event);
  const deliveries = effectDeliveries.get(delivery.effect);
  deliveries?.delete(delivery);
  if (!deliveries?.size) effectDeliveries.delete(delivery.effect);
}

/** @internal Registers the originating effect before an asynchronous handoff. */
export function registerPendingDelivery(
  effect: ExecutableEffect | undefined,
  source: AnyActor | undefined,
  target: AnyActor,
  event: EventObject
): () => void {
  if (
    !effect ||
    effect.kind !== 'builtin' ||
    effect.type !== '@xstate.sendTo' ||
    effect.delay !== undefined ||
    effect.source !== source ||
    effect.target !== target ||
    effect.event !== event
  )
    return () => {};
  const delivery = { effect, source, target, event };
  const list = pendingDeliveries.get(event) ?? [];
  list.push(delivery);
  pendingDeliveries.set(event, list);
  const deliveries = effectDeliveries.get(effect) ?? new Set<PendingDelivery>();
  deliveries.add(delivery);
  effectDeliveries.set(effect, deliveries);
  return () => removePendingDelivery(delivery);
}

function sameDeliveryTarget(expected: AnyActor, actual: AnyActor): boolean {
  if (expected === actual) return true;
  if (isSerializedActorRef(expected))
    return (
      expected.$actor === actual.address &&
      (expected.incarnation === undefined ||
        expected.incarnation === actual.sessionId)
    );
  return false;
}

/** @internal Accepts the earliest matching handoff before the recipient processes it. */
export function acceptPendingDelivery(
  source: AnyActor | undefined,
  target: AnyActor,
  event: EventObject
): boolean {
  const delivery = pendingDeliveries
    .get(event)
    ?.find(
      (item) =>
        item.source === source && sameDeliveryTarget(item.target, target)
    );
  if (!delivery) return false;
  removePendingDelivery(delivery);
  acceptPendingEffect(delivery.effect);
  return true;
}

export function withPendingEffect<T>(effect: ExecutableEffect, fn: () => T): T {
  const previous = executing;
  executing = effect;
  try {
    return fn();
  } finally {
    executing = previous;
  }
}

export function getExecutingPendingEffect(): ExecutableEffect | undefined {
  return executing;
}

export function setPendingEffects(
  actor: AnyActor,
  effects: readonly ExecutableEffect[]
): void {
  if (!effects.length) return;
  const batch = pending.get(actor) ?? [];
  for (const effect of effects) {
    owners.set(effect, actor);
    if (!batch.includes(effect)) batch.push(effect);
  }
  pending.set(actor, batch);
}

export function acceptPendingEffect(
  effect: ExecutableEffect | undefined
): void {
  if (!effect) return;
  for (const delivery of effectDeliveries.get(effect) ?? [])
    removePendingDelivery(delivery);
  const actor = owners.get(effect);
  if (!actor) return;
  const batch = pending.get(actor);
  if (!batch) return;
  const index = batch.indexOf(effect);
  if (index !== -1) batch.splice(index, 1);
  if (!batch.length) pending.delete(actor);
}

/** @internal Discards a failed synchronous effect and its unstarted tail. */
export function discardPendingSuffix(effect: ExecutableEffect): void {
  const actor = owners.get(effect);
  const batch = actor && pending.get(actor);
  const index = batch?.indexOf(effect) ?? -1;
  if (!batch || index < 0) return;
  for (const item of batch.slice(index)) acceptPendingEffect(item);
}

function sourcesOf(actor: AnyActor): Record<string, Record<string, unknown>> {
  return (
    (
      actor.getSnapshot() as {
        machine?: { sources: Record<string, Record<string, unknown>> };
      }
    ).machine?.sources ?? {}
  );
}

const nativeErrorStackGetter = Object.getOwnPropertyDescriptor(
  new Error(),
  'stack'
)?.get;

const errorKinds: Record<string, ErrorConstructor> = {
  Error,
  TypeError,
  RangeError,
  SyntaxError,
  ReferenceError,
  URIError,
  EvalError
};

export function persistEffectValue(value: unknown, actor: AnyActor): unknown {
  return encodeArgument(value, actor);
}

export function restoreEffectValue(value: unknown, actor: AnyActor): unknown {
  if (!isEncoded(value, new Set()))
    throw new Error('Invalid persisted effect value.');
  let root = actor;
  while (root._parent) root = root._parent;
  return decodeArgument(value, actor, root);
}

function isEncoded(value: unknown, seen: Set<object>): value is Encoded {
  if (
    !value ||
    typeof value !== 'object' ||
    seen.has(value) ||
    Array.isArray(value)
  )
    return false;
  seen.add(value);
  const record = value as Record<string, unknown>;
  const nested = (v: unknown) => isEncoded(v, seen);
  switch (record.tag) {
    case 'undefined':
    case 'system':
      return true;
    case 'value':
      return (
        record.value === null ||
        typeof record.value === 'string' ||
        typeof record.value === 'boolean' ||
        (typeof record.value === 'number' &&
          Number.isFinite(record.value) &&
          !Object.is(record.value, -0))
      );
    case 'ref':
      return Number.isSafeInteger(record.id) && (record.id as number) >= 0;
    case 'actor':
      return typeof record.address === 'string';
    case 'source':
      return typeof record.group === 'string' && typeof record.key === 'string';
    case 'array':
      return (
        Number.isSafeInteger(record.id) &&
        (record.id as number) >= 0 &&
        Array.isArray(record.value) &&
        record.value.every(nested)
      );
    case 'object':
      return (
        Number.isSafeInteger(record.id) &&
        (record.id as number) >= 0 &&
        typeof record.nullPrototype === 'boolean' &&
        Array.isArray(record.value) &&
        record.value.every(
          (entry: unknown) =>
            Array.isArray(entry) &&
            entry.length === 2 &&
            typeof entry[0] === 'string' &&
            nested(entry[1])
        )
      );
    case 'error':
      return (
        Number.isSafeInteger(record.id) &&
        (record.id as number) >= 0 &&
        typeof record.kind === 'string' &&
        Object.hasOwn(errorKinds, record.kind) &&
        Array.isArray(record.properties) &&
        record.properties.every(
          (entry: unknown) =>
            Array.isArray(entry) &&
            entry.length === 5 &&
            typeof entry[0] === 'string' &&
            nested(entry[1]) &&
            entry.slice(2).every((flag: unknown) => typeof flag === 'boolean')
        )
      );
    case 'machineSnapshot':
      return (
        Number.isSafeInteger(record.id) &&
        (record.id as number) >= 0 &&
        typeof record.address === 'string' &&
        Array.isArray(record.nodes) &&
        record.nodes.every((id: unknown) => typeof id === 'string') &&
        Array.isArray(record.history) &&
        record.history.every(
          (entry: unknown) =>
            Array.isArray(entry) &&
            entry.length === 2 &&
            typeof entry[0] === 'string' &&
            Array.isArray(entry[1]) &&
            entry[1].every((id: unknown) => typeof id === 'string')
        ) &&
        nested(record.data)
      );
    default:
      return false;
  }
}

function encodeArgument(
  value: unknown,
  actor: AnyActor,
  ancestors = new Set<object>(),
  graph = { ids: new WeakMap<object, number>(), next: 0 }
): Encoded {
  if (value === undefined) return { tag: 'undefined' };
  if (
    value === null ||
    typeof value === 'string' ||
    typeof value === 'boolean' ||
    (typeof value === 'number' &&
      Number.isFinite(value) &&
      !Object.is(value, -0))
  )
    return { tag: 'value', value };
  if (value === actor.system) return { tag: 'system' };
  for (const [group, entries] of Object.entries(sourcesOf(actor))) {
    if (value === entries) return { tag: 'source', group, key: '' };
    for (const [key, source] of Object.entries(entries))
      if (
        value === source &&
        (typeof value === 'function' || (value && typeof value === 'object'))
      )
        return { tag: 'source', group, key };
  }
  if (
    value &&
    typeof value === 'object' &&
    'address' in value &&
    'getSnapshot' in value &&
    typeof value.address === 'string' &&
    typeof value.getSnapshot === 'function'
  ) {
    let root = actor;
    while (root._parent) root = root._parent;
    if (!sameActor(findActor(root, value.address), value)) {
      throw new Error(
        'Unable to persist pending action argument: actor is no longer addressable.'
      );
    }
    return { tag: 'actor', address: value.address };
  }
  if (!value || typeof value !== 'object' || ancestors.has(value))
    throw new Error('Unable to persist pending action argument.');
  const existingId = graph.ids.get(value);
  if (existingId !== undefined) return { tag: 'ref', id: existingId };
  const id = graph.next++;
  graph.ids.set(value, id);
  if (value instanceof Error) {
    const kind = Object.keys(errorKinds).find(
      (key) => Object.getPrototypeOf(value) === errorKinds[key].prototype
    );
    if (!kind || Object.getOwnPropertySymbols(value).length)
      throw new Error('Unable to persist unsupported error.');
    ancestors.add(value);
    const properties: [string, Encoded, boolean, boolean, boolean][] = [];
    for (const [key, property] of Object.entries(
      Object.getOwnPropertyDescriptors(value)
    )) {
      const nativeStack =
        key === 'stack' &&
        nativeErrorStackGetter !== undefined &&
        property.get === nativeErrorStackGetter;
      if (!('value' in property) && !nativeStack)
        throw new Error('Unable to persist error accessor.');
      properties.push([
        key,
        encodeArgument(
          nativeStack ? nativeErrorStackGetter.call(value) : property.value,
          actor,
          ancestors,
          graph
        ),
        !!property.enumerable,
        nativeStack || !!property.writable,
        !!property.configurable
      ]);
    }
    ancestors.delete(value);
    return { tag: 'error', id, kind, properties };
  }
  const snapshotRef = getSnapshotActorRef(value as Snapshot<unknown>);
  if (snapshotRef && 'machine' in value && 'nodes' in value) {
    const snapshot = value as AnyMachineSnapshot;
    const address = snapshotRef.actor.address;
    let root = actor;
    while (root._parent) root = root._parent;
    if (!sameActor(findActor(root, address), snapshotRef.actor))
      throw new Error(
        'Unable to persist machine snapshot: actor is no longer addressable.'
      );
    ancestors.add(value);
    const data = encodeArgument(
      {
        status: snapshot.status,
        output: snapshot.output,
        error: snapshot.error,
        context: snapshot.context,
        children: snapshot.children,
        timers: snapshot.timers,
        _stateInputs: snapshot._stateInputs,
        _nextTimerId: snapshot._nextTimerId,
        _nextActorIds: snapshot._nextActorIds
      },
      actor,
      ancestors,
      graph
    );
    ancestors.delete(value);
    return {
      tag: 'machineSnapshot',
      id,
      address,
      nodes: snapshot.nodes.map((node) => node.id),
      history: Object.entries(snapshot.historyValue ?? {}).map(
        ([key, nodes]) => [key, nodes.map((node) => node.id)]
      ),
      data
    };
  }
  const prototype = Object.getPrototypeOf(value);
  if (
    !Array.isArray(value) &&
    prototype !== Object.prototype &&
    prototype !== null
  )
    throw new Error('Unable to persist pending action argument.');
  const properties = Object.getOwnPropertyDescriptors(value);
  if (
    Object.getOwnPropertySymbols(value).length ||
    Object.entries(properties).some(
      ([key, property]) =>
        (key !== 'length' || !Array.isArray(value)) &&
        (!property.enumerable ||
          (!('value' in property) &&
            !hasLazyActorCapability(value, key, property.get)))
    )
  )
    throw new Error('Unable to persist pending action argument.');
  if (
    Array.isArray(value) &&
    (Object.keys(value).length !== value.length ||
      Array.from({ length: value.length }, (_, index) => index).some(
        (index) => !Object.hasOwn(value, index)
      ))
  ) {
    throw new Error('Unable to persist pending action argument.');
  }
  ancestors.add(value);
  const encoded: Encoded = Array.isArray(value)
    ? {
        tag: 'array',
        id,
        value: value.map((item) =>
          encodeArgument(item, actor, ancestors, graph)
        )
      }
    : {
        tag: 'object',
        id,
        nullPrototype: prototype === null,
        value: Object.entries(properties).map(([key, property]) => [
          key,
          encodeArgument(
            'value' in property
              ? property.value
              : key === 'self'
                ? actor
                : key === 'parent'
                  ? actor._parent
                  : actor.system,
            actor,
            ancestors,
            graph
          )
        ])
      };
  ancestors.delete(value);
  return encoded;
}

function decodeArgument(
  value: Encoded,
  actor: AnyActor,
  root: AnyActor,
  graph = new Map<number, unknown>()
): unknown {
  switch (value.tag) {
    case 'value':
      return value.value;
    case 'undefined':
      return undefined;
    case 'ref':
      if (!graph.has(value.id))
        throw new Error('Invalid persisted effect value reference.');
      return graph.get(value.id);
    case 'system':
      return actor.system;
    case 'actor': {
      const found = findActor(root, value.address);
      if (!found)
        throw new Error(
          'Unable to restore pending action argument: actor is no longer addressable.'
        );
      return found;
    }
    case 'source': {
      const sources = sourcesOf(actor);
      const entries = Object.hasOwn(sources, value.group)
        ? sources[value.group]
        : undefined;
      const found =
        value.key && entries && Object.hasOwn(entries, value.key)
          ? entries[value.key]
          : value.key
            ? undefined
            : entries;
      if (found === undefined)
        throw new Error('Unable to restore pending action source.');
      return found;
    }
    case 'error': {
      const error = new errorKinds[value.kind]();
      if (graph.has(value.id))
        throw new Error('Duplicate persisted effect value identity.');
      graph.set(value.id, error);
      for (const [
        key,
        item,
        enumerable,
        writable,
        configurable
      ] of value.properties) {
        Object.defineProperty(error, key, {
          value: decodeArgument(item, actor, root, graph),
          enumerable,
          writable,
          configurable
        });
      }
      return error;
    }
    case 'machineSnapshot': {
      const owner = findActor(root, value.address);
      const current = owner?.getSnapshot();
      if (!current || !('machine' in current))
        throw new Error('Unable to restore machine snapshot owner.');
      const snapshot = current as AnyMachineSnapshot;
      const data = decodeArgument(value.data, actor, root, graph);
      if (!data || typeof data !== 'object' || Array.isArray(data))
        throw new Error('Invalid persisted machine snapshot data.');
      const restoredSnapshot = cloneMachineSnapshot(snapshot, {
        ...data,
        historyValue: Object.fromEntries(
          value.history.map(([key, ids]) => [
            key,
            ids.map((id) => snapshot.machine.getStateNodeById(id))
          ])
        ),
        _nodes: value.nodes.map((id) => snapshot.machine.getStateNodeById(id))
      });
      if (graph.has(value.id))
        throw new Error('Duplicate persisted effect value identity.');
      graph.set(value.id, restoredSnapshot);
      return restoredSnapshot;
    }
    case 'array': {
      if (graph.has(value.id))
        throw new Error('Duplicate persisted effect value identity.');
      const result: unknown[] = [];
      graph.set(value.id, result);
      for (const item of value.value)
        result.push(decodeArgument(item, actor, root, graph));
      return result;
    }
    case 'object': {
      if (graph.has(value.id))
        throw new Error('Duplicate persisted effect value identity.');
      const result: Record<string, unknown> = value.nullPrototype
        ? Object.create(null)
        : {};
      graph.set(value.id, result);
      for (const [key, item] of value.value)
        Object.defineProperty(result, key, {
          value: decodeArgument(item, actor, root, graph),
          writable: true,
          configurable: true,
          enumerable: true
        });
      return result;
    }
  }
}

export function hasPendingChildStart(
  owner: AnyActor,
  child: AnyActor
): boolean {
  return !!pending
    .get(owner)
    ?.some(
      (effect) =>
        effect.kind === 'builtin' &&
        (effect.type === '@xstate.start' || effect.type === '@xstate.spawn') &&
        effect.actor === child
    );
}

export function discardPendingEffects(actor: AnyActor): void {
  for (const effect of [...(pending.get(actor) ?? [])])
    acceptPendingEffect(effect);
  restored.delete(actor);
}

export function persistPendingEffects(
  actor: AnyActor,
  embedChildren = true
): PendingDescriptor[] | undefined {
  const saved = restored.get(actor);
  if (saved?.length) {
    if (embedChildren) return saved;
    const children =
      (actor.getSnapshot() as { children?: Record<string, AnyActor> })
        .children ?? {};
    const addresses = new Set(
      Object.values(children).map((child) => child.address)
    );
    const retained = saved.filter(
      (entry) =>
        !(
          entry.kind === 'builtin' &&
          (entry.type === '@xstate.start' || entry.type === '@xstate.spawn') &&
          addresses.has(entry.actor.$actor)
        )
    );
    return retained.length ? retained : undefined;
  }
  const batch = pending.get(actor)?.filter((effect) => {
    if (
      !embedChildren &&
      effect.kind === 'builtin' &&
      (effect.type === '@xstate.start' || effect.type === '@xstate.spawn') &&
      effect.actor._parent === actor
    )
      return false;
    return !isRestartedStepEffect(effect);
  });
  if (!batch?.length) return undefined;
  let root = actor;
  while (root._parent) root = root._parent;
  const graph = { ids: new WeakMap<object, number>(), next: 0 };
  return batch.map((effect): PendingDescriptor => {
    if (effect.kind === 'builtin') {
      const target =
        effect.type === '@xstate.sendTo'
          ? effect.target
          : effect.type === '@xstate.start' ||
              effect.type === '@xstate.spawn' ||
              effect.type === '@xstate.stop' ||
              effect.type === '@xstate.terminate'
            ? effect.actor
            : undefined;
      if (
        target &&
        effect.type !== '@xstate.sendTo' &&
        !sameActor(findActor(root, target.address), target)
      ) {
        throw new Error(
          `Unable to persist pending effect '${effect.type}': actor is no longer addressable.`
        );
      }
    }
    if (
      effect.kind === 'builtin' &&
      effect.type === '@xstate.sendTo' &&
      effect.target &&
      !isSerializedActorRef(effect.target) &&
      !isRemoteActorRef(effect.target)
    ) {
      const current = findActor(root, effect.target.address);
      if (current && !sameActor(current, effect.target))
        throw new Error(
          `Unable to persist pending effect '${effect.type}': actor is no longer addressable.`
        );
    }
    if (effect.kind === 'action') {
      const sources = (
        actor.getSnapshot() as {
          machine?: { sources: { actions: Record<string, unknown> } };
        }
      ).machine?.sources.actions;
      const name = Object.keys(sources ?? {}).find(
        (key) => sources?.[key] === effect.action
      );
      if (!name)
        throw new Error(`Unable to persist pending effect '${effect.type}'.`);
      return {
        kind: 'registeredAction',
        source: name,
        args: encodeArgument(effect.args, actor, new Set(), graph),
        params: encodeArgument(effect.params, actor, new Set(), graph)
      };
    }
    if (
      effect.kind === 'builtin' &&
      ((effect.type === '@xstate.sendTo' &&
        effect.delay === undefined &&
        effect.target) ||
        effect.type === '@xstate.spawn' ||
        effect.type === '@xstate.start' ||
        effect.type === '@xstate.terminate' ||
        effect.type === '@xstate.raise' ||
        effect.type === '@xstate.cancel' ||
        effect.type === '@xstate.stop' ||
        (effect.type === '@xstate.sendTo' && effect.target))
    ) {
      const described = describeExecutableEffect(effect);
      if (described.kind !== 'builtin')
        throw new Error(`Unable to persist pending effect '${effect.type}'.`);
      if (
        described.type === '@xstate.sendTo' ||
        described.type === '@xstate.raise'
      ) {
        const descriptor: PendingEventDescriptor = {
          kind: 'builtin',
          type: described.type,
          source: described.source,
          ...(described.type === '@xstate.sendTo' &&
            described.target !== undefined && { target: described.target }),
          ...(described.id !== undefined && { id: described.id }),
          ...(described.seq !== undefined && { seq: described.seq }),
          ...(described.delay !== undefined && { delay: described.delay })
        };
        return {
          kind: 'builtinWithEvent',
          descriptor,
          event: encodeArgument(described.event, actor, new Set(), graph)
        };
      }
      if (described.type === '@xstate.cancel') {
        const { event, ...descriptor } = described;
        return event === undefined
          ? descriptor
          : {
              kind: 'builtinWithEvent',
              descriptor,
              event: encodeArgument(event, actor, new Set(), graph)
            };
      }
      if (described.type === '@xstate.stop')
        return {
          kind: 'builtin',
          type: described.type,
          actor: described.actor,
          source: described.source
        };
      if (
        described.type === '@xstate.spawn' ||
        described.type === '@xstate.start' ||
        described.type === '@xstate.terminate'
      )
        return {
          kind: 'builtin',
          type: described.type,
          actor: described.actor
        };
      throw new Error(`Unable to persist pending effect '${effect.type}'.`);
    }
    throw new Error(`Unable to persist pending effect '${effect.type}'.`);
  });
}

export function hasRestoredPendingEffects(actor: AnyActor): boolean {
  return !!restored.get(actor)?.length;
}

export function restorePendingEffects(
  actor: AnyActor,
  descriptors: PendingDescriptor[] | undefined
): void {
  if (descriptors?.length) restored.set(actor, descriptors);
}

function sameActor(current: AnyActor | undefined, candidate: unknown): boolean {
  if (current === candidate) return true;
  if (
    !current ||
    !candidate ||
    typeof candidate !== 'object' ||
    !('address' in candidate) ||
    !('sessionId' in candidate)
  )
    return false;
  return (
    typeof candidate.address === 'string' &&
    typeof candidate.sessionId === 'string' &&
    current.address === candidate.address &&
    current.sessionId === candidate.sessionId
  );
}

function findActor(actor: AnyActor, address: string): AnyActor | undefined {
  if (actor.address === address) return actor;
  if (isRemoteActorRef(actor)) return undefined;
  const children = (
    actor.getSnapshot() as { children?: Record<string, AnyActor | undefined> }
  ).children;
  for (const child of Object.values(children ?? {})) {
    const found = child && findActor(child, address);
    if (found) return found;
  }
  return undefined;
}

function resolveSendTarget(root: AnyActor, ref: SerializedActorRef): AnyActor {
  const current = findActor(root, ref.$actor);
  if (
    current &&
    (ref.incarnation === undefined || current.sessionId === ref.incarnation)
  )
    return current;
  return ref as unknown as AnyActor;
}

export function hydratePendingEffects(actor: AnyActor): ExecutableEffect[] {
  const descriptors = restored.get(actor);
  if (!descriptors?.length) return [];
  let root = actor;
  while (root._parent) root = root._parent;
  if (actor.getSnapshot().status === 'stopped') {
    restored.delete(actor);
    return [];
  }
  const graph = new Map<number, unknown>();
  const executable = descriptors.filter((descriptor) => {
    if (
      descriptor.kind !== 'builtin' ||
      (descriptor.type !== '@xstate.start' &&
        descriptor.type !== '@xstate.spawn')
    )
      return true;
    if (actor.getSnapshot().status !== 'active') return false;
    const child = findActor(root, descriptor.actor.$actor);
    if (!child || isRemoteActorRef(child)) return false;
    if (child.getSnapshot().status === 'active') return true;
    return (
      descriptor.type === '@xstate.start' && hasRestoredPendingEffects(child)
    );
  });
  const effects = executable.map((savedDescriptor) => {
    const descriptor =
      savedDescriptor.kind === 'builtinWithEvent'
        ? {
            ...savedDescriptor.descriptor,
            event: decodeArgument(
              savedDescriptor.event,
              actor,
              root,
              graph
            ) as EventObject
          }
        : savedDescriptor;
    if (descriptor.kind === 'registeredAction') {
      const sources = (
        actor.getSnapshot() as {
          machine?: { sources: { actions: Record<string, unknown> } };
        }
      ).machine?.sources.actions;
      const action =
        sources && Object.hasOwn(sources, descriptor.source)
          ? sources[descriptor.source]
          : undefined;
      if (typeof action !== 'function')
        throw new Error(
          `Unable to restore registered action '${descriptor.source}'.`
        );
      return createRegisteredActionEffect(
        descriptor.source,
        action as (...args: unknown[]) => void,
        decodeArgument(descriptor.args, actor, root, graph) as unknown[],
        decodeArgument(descriptor.params, actor, root, graph)
      );
    }
    if (
      descriptor.kind === 'builtin' &&
      (descriptor.type === '@xstate.start' ||
        descriptor.type === '@xstate.spawn')
    ) {
      const child = findActor(root, descriptor.actor.$actor);
      if (!child)
        throw new Error(
          `Unable to restore pending effect '${descriptor.type}': actor is no longer addressable.`
        );
      return descriptor.type === '@xstate.start'
        ? createStartEffect(child)
        : createSpawnEffect(child);
    }
    if (
      descriptor.kind === 'builtin' &&
      descriptor.type === '@xstate.terminate'
    ) {
      const target = findActor(root, descriptor.actor.$actor);
      if (!target)
        throw new Error(
          'Unable to restore pending termination: actor is no longer addressable.'
        );
      return createTerminationEffect(
        target as unknown as AnyActorScope,
        target.getSnapshot()
      );
    }
    if (descriptor.kind === 'builtin' && descriptor.type === '@xstate.stop') {
      const source = findActor(root, descriptor.source.$actor);
      const target = findActor(root, descriptor.actor.$actor);
      if (!source || !target)
        throw new Error(
          'Unable to restore pending stop: actor is no longer addressable.'
        );
      return createStopEffect(source as unknown as AnyActorScope, target);
    }
    if (descriptor.kind === 'builtin' && descriptor.type === '@xstate.cancel') {
      const source = findActor(root, descriptor.source.$actor);
      const key = readTimerKey(descriptor);
      if (!source || !key)
        throw new Error('Unable to restore pending cancellation.');
      return createCancelEffect(source as unknown as AnyActorScope, key);
    }
    if (
      descriptor.kind === 'builtin' &&
      (descriptor.type === '@xstate.raise' ||
        (descriptor.type === '@xstate.sendTo' &&
          descriptor.delay !== undefined))
    ) {
      const source = findActor(root, descriptor.source.$actor);
      const target =
        descriptor.type === '@xstate.raise'
          ? source
          : descriptor.target && resolveSendTarget(root, descriptor.target);
      if (!source || !target)
        throw new Error(
          'Unable to restore pending timer: actor is no longer addressable.'
        );
      const delay = descriptor.delay ?? 0;
      return createTimerEffect(
        source as unknown as AnyActorScope,
        {
          type: descriptor.type,
          id: descriptor.id,
          seq: descriptor.seq,
          event: descriptor.event as EventObject,
          delay,
          target
        },
        delay
      );
    }
    if (
      descriptor.kind !== 'builtin' ||
      descriptor.type !== '@xstate.sendTo' ||
      descriptor.delay !== undefined ||
      !descriptor.target
    ) {
      throw new Error(`Unable to restore pending effect '${descriptor.type}'.`);
    }
    const source = findActor(root, descriptor.source.$actor);
    const target = resolveSendTarget(root, descriptor.target);
    if (!source || !target)
      throw new Error(
        'Unable to restore pending send: actor is no longer addressable.'
      );
    return createSendToEffect(
      source as unknown as AnyActorScope,
      target,
      descriptor.event as EventObject
    );
  });
  restored.delete(actor);
  setPendingEffects(actor, effects);
  return effects;
}
