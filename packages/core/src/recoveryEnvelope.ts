/** Internal recovery metadata shared by actor runtimes. */
export type RecoveryEnvelope = {
  type: 'xstate.recovery';
  version: 1;
  effects?: unknown[];
  mailbox?: unknown[];
  effectMailbox?: unknown[];
  durableMailbox?: unknown[];
  effectSessions?: Record<string, string | undefined>;
};

const generated = new WeakSet<object>();

export function readRecoveryEnvelope(
  snapshot: unknown
): RecoveryEnvelope | undefined {
  if (!snapshot || typeof snapshot !== 'object' || Array.isArray(snapshot))
    return undefined;
  const value = (snapshot as Record<string, unknown>)._xstateRecovery;
  if (
    !value ||
    typeof value !== 'object' ||
    Array.isArray(value) ||
    (value as Record<string, unknown>).type !== 'xstate.recovery'
  )
    return undefined;
  const envelope = value as Record<string, unknown>;
  if (envelope.version !== 1)
    throw new Error('Unsupported recovery metadata version.');
  if (
    (envelope.effects !== undefined && !Array.isArray(envelope.effects)) ||
    (envelope.mailbox !== undefined && !Array.isArray(envelope.mailbox)) ||
    (envelope.effectMailbox !== undefined &&
      !Array.isArray(envelope.effectMailbox)) ||
    (envelope.durableMailbox !== undefined &&
      !Array.isArray(envelope.durableMailbox)) ||
    (envelope.effectSessions !== undefined &&
      (!envelope.effectSessions ||
        typeof envelope.effectSessions !== 'object' ||
        Array.isArray(envelope.effectSessions) ||
        Object.values(envelope.effectSessions).some(
          (v) => v !== undefined && typeof v !== 'string'
        )))
  )
    throw new Error('Invalid recovery metadata.');
  return envelope as RecoveryEnvelope;
}

export function addRecoveryEnvelope<T>(
  snapshot: T,
  fields: Partial<Omit<RecoveryEnvelope, 'type' | 'version'>>
): T {
  if (!snapshot || typeof snapshot !== 'object' || Array.isArray(snapshot))
    throw new Error(
      'Unable to persist recovery metadata for this actor snapshot.'
    );
  if (Object.hasOwn(snapshot, '_xstateRecovery') && !generated.has(snapshot))
    throw new Error(
      'Unable to persist recovery metadata: _xstateRecovery is already in use.'
    );
  const previous = generated.has(snapshot)
    ? readRecoveryEnvelope(snapshot)
    : undefined;
  const result = {
    ...snapshot,
    _xstateRecovery: {
      type: 'xstate.recovery',
      version: 1,
      ...previous,
      ...fields
    }
  };
  generated.add(result);
  return result as T;
}

export function stripRecoveryEnvelope(snapshot: unknown): unknown {
  if (!readRecoveryEnvelope(snapshot)) return snapshot;
  const { _xstateRecovery: _, ...rest } = snapshot as Record<string, unknown>;
  return rest;
}
