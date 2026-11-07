import { isMachineSnapshot } from './State.ts';
import { restoreChildLifecycleEvent } from './eventUtils.ts';
import { persistEffectValue, restoreEffectValue } from './pendingEffects.ts';
import type { AnyActor, AnyEventObject } from './types.ts';

type DurableMailboxItem = {
  event: AnyEventObject;
  source: AnyActor | undefined;
};
const readers = new WeakMap<AnyActor, () => readonly DurableMailboxItem[]>();

/** @internal Associates the owning runtime's root queue with its actor. */
export function registerDurableMailbox(
  actor: AnyActor,
  read: () => readonly DurableMailboxItem[]
): void {
  readers.set(actor, read);
}

/** @internal Captures all root events not yet committed by a transition. */
export function persistDurableMailbox(actor: AnyActor): unknown[] | undefined {
  const items = readers.get(actor)?.();
  if (!items?.length) return undefined;
  const children = (
    actor.getSnapshot() as { children?: Record<string, AnyActor | undefined> }
  ).children;
  const snapshot = actor.getSnapshot();
  const encoded = items.flatMap(({ event, source }) => {
    const actorId = event.actorId;
    const currentChildSession =
      typeof actorId === 'string' ? children?.[actorId]?.sessionId : undefined;
    if (
      !restoreChildLifecycleEvent(
        event,
        currentChildSession,
        currentChildSession,
        isMachineSnapshot(snapshot)
      )
    )
      return [];
    return [persistEffectValue({ event, source, currentChildSession }, actor)];
  });
  return encoded.length ? encoded : undefined;
}

/** @internal Rebinds queued events and current-child incarnation IDs. */
export function restoreDurableMailbox(
  actor: AnyActor,
  items: unknown[]
): DurableMailboxItem[] {
  const children = (
    actor.getSnapshot() as { children?: Record<string, AnyActor | undefined> }
  ).children;
  return items.flatMap((encoded) => {
    const value = restoreEffectValue(encoded, actor);
    if (!value || typeof value !== 'object' || Array.isArray(value))
      throw new Error('Invalid persisted durable mailbox event.');
    const item = value as {
      event?: unknown;
      source?: unknown;
      currentChildSession?: unknown;
    };
    if (
      !item.event ||
      typeof item.event !== 'object' ||
      Array.isArray(item.event) ||
      typeof (item.event as { type?: unknown }).type !== 'string' ||
      (item.source !== undefined &&
        (!item.source ||
          typeof item.source !== 'object' ||
          typeof (item.source as { _send?: unknown })._send !== 'function')) ||
      (item.currentChildSession !== undefined &&
        typeof item.currentChildSession !== 'string')
    )
      throw new Error('Invalid persisted durable mailbox event.');
    const event = item.event as AnyEventObject;
    const child =
      typeof event.actorId === 'string' ? children?.[event.actorId] : undefined;
    const restoredEvent = restoreChildLifecycleEvent(
      event,
      item.currentChildSession as string | undefined,
      child?.sessionId,
      isMachineSnapshot(actor.getSnapshot())
    );
    return restoredEvent
      ? [{ event: restoredEvent, source: item.source as AnyActor | undefined }]
      : [];
  });
}
