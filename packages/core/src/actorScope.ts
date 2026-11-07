import type { AnyActor, AnyActorScope } from './types.ts';

/** Marks the internal lazy scope without imposing structure on custom runtimes. @internal */
export const lazyActorScope = Symbol();

const lazyCapabilities = new WeakMap<object, Map<string, () => unknown>>();

function markLazyCapabilities(value: object, keys: readonly string[]): void {
  const getters = new Map<string, () => unknown>();
  for (const key of keys) {
    const getter = Object.getOwnPropertyDescriptor(value, key)?.get;
    if (getter) getters.set(key, getter);
  }
  lazyCapabilities.set(value, getters);
}

/** @internal Identifies framework-owned lazy transition argument accessors. */
export function hasLazyActorCapability(
  value: object,
  key: string,
  getter: (() => unknown) | undefined
): boolean {
  return (
    getter !== undefined && lazyCapabilities.get(value)?.get(key) === getter
  );
}

type InternalActorScope = AnyActorScope & {
  _parent?: AnyActor;
  [lazyActorScope]?: true;
};

/** Whether actor capabilities are represented by a lazy pure-transition scope. @internal */
export function isLazyActorScope(actorScope: AnyActorScope): boolean {
  return !!(actorScope as InternalActorScope)[lazyActorScope];
}

/** Reads parent metadata without otherwise touching actor capabilities. @internal */
export function getActorScopeParent(
  actorScope: AnyActorScope
): AnyActor | undefined {
  return isLazyActorScope(actorScope)
    ? (actorScope as InternalActorScope)._parent
    : actorScope.self._parent;
}

/** Adds only `self`, preserving callback surfaces that expose no other capabilities. @internal */
export function withActorSelf<T extends object>(
  args: T,
  actorScope: AnyActorScope
): T & Pick<AnyActorScope, 'self'> {
  if (!isLazyActorScope(actorScope)) {
    return Object.assign(args, { self: actorScope.self });
  }
  Object.defineProperty(args, 'self', {
    enumerable: true,
    get: () => actorScope.self
  });
  markLazyCapabilities(args, ['self']);
  return args as T & Pick<AnyActorScope, 'self'>;
}

/** Adds the actor reference and parent without exposing the system. @internal */
export function withActorSelfAndParent<T extends object>(
  args: T,
  actorScope: AnyActorScope
): T & Pick<AnyActorScope, 'self'> & { parent: AnyActor | undefined } {
  if (!isLazyActorScope(actorScope)) {
    const self = actorScope.self;
    return Object.assign(args, { self, parent: self._parent });
  }
  Object.defineProperties(args, {
    self: { enumerable: true, get: () => actorScope.self },
    parent: { enumerable: true, get: () => getActorScopeParent(actorScope) }
  });
  markLazyCapabilities(args, ['self', 'parent']);
  return args as T &
    Pick<AnyActorScope, 'self'> & { parent: AnyActor | undefined };
}

/** Adds actor capabilities without reading them until the callback does. @internal */
export function withActorScope<T extends object>(
  args: T,
  actorScope: AnyActorScope
): T &
  Pick<AnyActorScope, 'self' | 'system'> & {
    parent: AnyActor | undefined;
  } {
  if (!isLazyActorScope(actorScope)) {
    const actorArgs = args as T &
      Pick<AnyActorScope, 'self' | 'system'> & {
        parent: AnyActor | undefined;
      };
    const self = actorScope.self;
    actorArgs.self = self;
    actorArgs.system = actorScope.system;
    actorArgs.parent = self._parent;
    return actorArgs;
  }
  Object.defineProperties(args, {
    self: { enumerable: true, get: () => actorScope.self },
    system: { enumerable: true, get: () => actorScope.system },
    parent: { enumerable: true, get: () => getActorScopeParent(actorScope) }
  });
  markLazyCapabilities(args, ['self', 'system', 'parent']);
  return args as T &
    Pick<AnyActorScope, 'self' | 'system'> & {
      parent: AnyActor | undefined;
    };
}
