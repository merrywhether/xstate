import { Effect, Exit, Queue } from 'effect';
import { ActorStoppedError, EventRejectedError } from './errors.ts';
import { StateMachine } from 'xstate';
import type {
  ActorRef,
  AnyActor,
  AnyActorLogic,
  EmittedFrom,
  EventFromLogic,
  InspectionEvent,
  Observer,
  PersistedSnapshotOf,
  Snapshot,
  SnapshotFrom,
  Subscription
} from 'xstate';

export interface Receipt<TSnapshot> {
  resume?: (
    exit: Exit.Exit<TSnapshot, ActorStoppedError | EventRejectedError>
  ) => void;
}

export type MailboxItem<TEvent, TSnapshot> =
  | {
      readonly kind: 'event';
      readonly event: TEvent;
      readonly receipt?: Receipt<TSnapshot>;
    }
  | { readonly kind: 'actionFailure'; readonly error: unknown };

export type PersistedMailboxItem =
  | { kind: 'event'; event: unknown }
  | { kind: 'actionFailure'; error: unknown };

export function restoreMailboxItem<TEvent, TSnapshot>(
  item: PersistedMailboxItem,
  restoreValue: (value: unknown) => unknown
): MailboxItem<TEvent, TSnapshot> {
  switch (item.kind) {
    case 'event':
      return { kind: 'event', event: restoreValue(item.event) as TEvent };
    case 'actionFailure':
      return { kind: 'actionFailure', error: restoreValue(item.error) };
    default:
      throw new Error('Invalid persisted mailbox item.');
  }
}

const symbolObservable: typeof Symbol.observable = (() =>
  (typeof Symbol === 'function' && Symbol.observable) ||
  '@@observable')() as any;

/**
 * Calls a listener and reports an exception it throws without letting it
 * escape into the interpreter, matching core's `safeCall`.
 */
export function safeCall<T>(fn: ((arg: T) => void) | undefined, arg?: T) {
  try {
    fn?.(arg as T);
  } catch (err) {
    setTimeout(() => {
      throw err;
    });
  }
}

function toObserver<T>(
  nextHandler?: Observer<T> | ((value: T) => void),
  errorHandler?: (error: any) => void,
  completionHandler?: () => void
): Observer<T> {
  if (typeof nextHandler === 'object') {
    return nextHandler;
  }
  return {
    next: nextHandler,
    error: errorHandler,
    complete: completionHandler
  };
}

/**
 * The handle `createEffectActor` returns. It implements XState's `ActorRef`
 * contract (`send`, `getSnapshot`, `subscribe`, `on`), so it works with
 * `useSelector`, this package's actor functions and the inspection APIs, while
 * the actor itself is driven by an Effect fiber over pure transitions.
 *
 * @experimental
 */
export class EffectActor<TLogic extends AnyActorLogic> implements ActorRef<
  SnapshotFrom<TLogic>,
  EventFromLogic<TLogic>,
  EmittedFrom<TLogic>
> {
  readonly id: string;
  readonly address: string;
  readonly sessionId: string | undefined;
  /** The actor system that hosts this actor and its children. */
  readonly system: AnyActor['system'];
  /** @internal */
  readonly _root: AnyActor;

  private _snapshot: SnapshotFrom<TLogic>;
  private readonly _observers = new Set<Observer<SnapshotFrom<TLogic>>>();
  private readonly _listeners = new Map<
    string,
    Set<(emitted: EmittedFrom<TLogic>) => void>
  >();
  private _settled = false;
  private readonly _pending = new Set<Receipt<SnapshotFrom<TLogic>>>();

  constructor(
    readonly logic: TLogic,
    root: AnyActor,
    snapshot: SnapshotFrom<TLogic>,
    private readonly _mailbox: Queue.Queue<
      MailboxItem<EventFromLogic<TLogic>, SnapshotFrom<TLogic>>
    >,
    private readonly _mailboxItems: Set<
      MailboxItem<EventFromLogic<TLogic>, SnapshotFrom<TLogic>>
    >,
    private readonly _persistSnapshot: (
      snapshot: SnapshotFrom<TLogic>
    ) => PersistedSnapshotOf<TLogic>,
    private readonly _restoreLifecycleEvent: (
      event: EventFromLogic<TLogic>,
      capturedChildSession: string | undefined,
      snapshot: SnapshotFrom<TLogic>
    ) => EventFromLogic<TLogic> | undefined,
    private readonly _addRecovery: (
      snapshot: PersistedSnapshotOf<TLogic>,
      fields: {
        effectMailbox: PersistedMailboxItem[];
        effectSessions: Record<string, string | undefined>;
      }
    ) => PersistedSnapshotOf<TLogic>,
    private readonly _persistValue: (
      value: unknown,
      snapshot: SnapshotFrom<TLogic>
    ) => unknown,
    private readonly _stopExecution: () => void,
    private readonly _inspectors: Set<(event: InspectionEvent) => void>
  ) {
    this._root = root;
    this.id = root.id;
    this.address = root.address;
    this.sessionId = root.sessionId;
    this.system = root.system;
    this._snapshot = snapshot;
  }

  getSnapshot(): SnapshotFrom<TLogic> {
    return this._snapshot;
  }

  getPersistedSnapshot(): PersistedSnapshotOf<TLogic> {
    const children =
      (this._snapshot as { children?: Record<string, AnyActor | undefined> })
        .children ?? {};
    const events: PersistedMailboxItem[] = (
      (this._snapshot as Snapshot<unknown>).status === 'active'
        ? [...this._mailboxItems]
        : []
    ).flatMap((item): PersistedMailboxItem[] => {
      if (item.kind === 'actionFailure')
        return [
          {
            kind: 'actionFailure',
            error: this._persistValue(item.error, this._snapshot)
          }
        ];
      const actorId = (item.event as { actorId?: unknown }).actorId;
      const event = this._restoreLifecycleEvent(
        item.event,
        typeof actorId === 'string' ? children[actorId]?.sessionId : undefined,
        this._snapshot
      );
      return event === undefined
        ? []
        : [
            {
              kind: 'event',
              event: this._persistValue(event, this._snapshot)
            }
          ];
    });
    const persisted = this._persistSnapshot(this._snapshot);
    if (!events.length) return persisted;
    return this._addRecovery(persisted, {
      effectSessions: Object.fromEntries(
        Object.entries(children).map(([id, child]) => [id, child?.sessionId])
      ),
      effectMailbox: events
    });
  }

  /** Sends an event; the actor processes it on its own fiber. */
  send(event: EventFromLogic<TLogic>): void {
    if (this._deliveryError(event)) return;
    const item = { kind: 'event' as const, event };
    this._mailboxItems.add(item);
    Queue.offerUnsafe(this._mailbox, item);
  }

  /** @internal */
  _sendAndAwait(
    event: EventFromLogic<TLogic>
  ): Effect.Effect<
    SnapshotFrom<TLogic>,
    ActorStoppedError | EventRejectedError
  > {
    return Effect.callback((resume) => {
      const error = this._deliveryError(event);
      if (error) {
        resume(Effect.fail(error));
        return;
      }
      const receipt: Receipt<SnapshotFrom<TLogic>> = {
        resume: (exit) => resume(exit)
      };
      this._pending.add(receipt);
      const item = { kind: 'event' as const, event, receipt };
      this._mailboxItems.add(item);
      Queue.offerUnsafe(this._mailbox, item);
      return Effect.sync(() => {
        this._pending.delete(receipt);
        receipt.resume = undefined;
      });
    });
  }

  private _deliveryError(
    event: EventFromLogic<TLogic>
  ): ActorStoppedError | EventRejectedError | undefined {
    if (this._settled) {
      this.system.deadLetter(undefined, this._root, event, 'stopped');
      return this._stoppedError();
    }
    if (
      this.logic instanceof StateMachine &&
      this.logic.isInternalEventType(event.type)
    ) {
      this.system.deadLetter(undefined, this._root, event, 'internalEvent');
      return new EventRejectedError({
        actorId: this.id,
        event,
        reason: 'internalEvent'
      });
    }
    return undefined;
  }

  /** @internal */
  _rejectReceipts(): void {
    for (const receipt of this._pending)
      this._completeReceipt(receipt, Exit.fail(this._stoppedError()));
  }

  private _stoppedError(): ActorStoppedError {
    return new ActorStoppedError({
      actorId: this.id,
      snapshot: this._snapshot
    });
  }

  /** @internal */
  _completeReceipt(
    receipt: Receipt<SnapshotFrom<TLogic>> | undefined,
    exit: Exit.Exit<
      SnapshotFrom<TLogic>,
      ActorStoppedError | EventRejectedError
    >
  ): void {
    if (!receipt || !this._pending.delete(receipt)) return;
    const resume = receipt.resume;
    receipt.resume = undefined;
    resume?.(exit);
  }

  /** Stops the actor, its children and every Effect it hosts. */
  stop(): this {
    this._stopExecution();
    return this;
  }

  subscribe(observer: Observer<SnapshotFrom<TLogic>>): Subscription;
  subscribe(
    nextListener?: (snapshot: SnapshotFrom<TLogic>) => void,
    errorListener?: (error: any) => void,
    completeListener?: () => void
  ): Subscription;
  subscribe(
    nextListenerOrObserver?:
      | ((snapshot: SnapshotFrom<TLogic>) => void)
      | Observer<SnapshotFrom<TLogic>>,
    errorListener?: (error: any) => void,
    completeListener?: () => void
  ): Subscription {
    const observer = toObserver(
      nextListenerOrObserver,
      errorListener,
      completeListener
    );
    if (this._settled) {
      const snapshot = this._snapshot as Snapshot<unknown>;
      if (snapshot.status === 'error') {
        safeCall(observer.error, snapshot.error);
      } else {
        safeCall(observer.complete);
      }
      return { unsubscribe: () => {} };
    }
    this._observers.add(observer);
    return {
      unsubscribe: () => {
        this._observers.delete(observer);
      }
    };
  }

  on<TType extends EmittedFrom<TLogic>['type'] | '*'>(
    type: TType,
    handler: (
      emitted: EmittedFrom<TLogic> &
        (TType extends '*' ? unknown : { type: TType })
    ) => void
  ): Subscription {
    let listeners = this._listeners.get(type);
    if (!listeners) {
      listeners = new Set();
      this._listeners.set(type, listeners);
    }
    const listener = handler as (emitted: EmittedFrom<TLogic>) => void;
    listeners.add(listener);
    return {
      unsubscribe: () => {
        listeners.delete(listener);
      }
    };
  }

  /** @internal Delivers an emitted event to `on` listeners. */
  _emit(event: EmittedFrom<TLogic>): void {
    const listeners = [
      ...(this._listeners.get(event.type) ?? []),
      ...(this._listeners.get('*') ?? [])
    ];
    for (const listener of listeners) {
      safeCall(listener, event);
    }
  }

  /**
   * Observes the inspection events of this actor and its children: every
   * actor creation and transition of the execution.
   */
  inspect(
    observer: Observer<InspectionEvent> | ((event: InspectionEvent) => void)
  ): Subscription {
    const handler =
      typeof observer === 'function'
        ? observer
        : (event: InspectionEvent) => observer.next?.(event);
    this._inspectors.add(handler);
    return {
      unsubscribe: () => {
        this._inspectors.delete(handler);
      }
    };
  }

  [symbolObservable]() {
    return this;
  }

  toJSON(): { $actor: string } {
    return { $actor: this.address };
  }

  /** @internal Publishes a snapshot produced by the execution loop. */
  _publish(
    snapshot: SnapshotFrom<TLogic>,
    receipt?: Receipt<SnapshotFrom<TLogic>>
  ): void {
    if (this._settled) return;
    this._snapshot = snapshot;
    const status = (snapshot as Snapshot<unknown>).status;
    if (status === 'active') {
      for (const observer of this._observers) {
        safeCall(observer.next, snapshot);
      }
      return;
    }
    this._settle(snapshot, receipt);
  }

  /** @internal Marks the actor stopped and notifies observers. */
  _settle(
    snapshot: SnapshotFrom<TLogic>,
    processing?: Receipt<SnapshotFrom<TLogic>>
  ): void {
    if (this._settled) {
      return;
    }
    this._snapshot = snapshot;
    this._settled = true;
    for (const receipt of this._pending) {
      if (receipt !== processing)
        this._completeReceipt(receipt, Exit.fail(this._stoppedError()));
    }
    const observers = [...this._observers];
    this._observers.clear();
    const status = (snapshot as Snapshot<unknown>).status;
    if (status === 'done') {
      for (const observer of observers) {
        safeCall(observer.next, snapshot);
      }
    }
    for (const observer of observers) {
      if (status === 'error') {
        safeCall(observer.error, (snapshot as Snapshot<unknown>).error);
      } else {
        safeCall(observer.complete);
      }
    }
  }

  /** @internal */
  get _isSettled(): boolean {
    return this._settled;
  }
}
