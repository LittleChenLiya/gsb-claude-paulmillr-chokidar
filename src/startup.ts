/*! chokidar - MIT License (c) 2012 Paul Miller (paulmillr.com) */
/**
 * Explicit startup adapter.
 *
 * Wraps any chokidar-compatible watcher and splits its output into two
 * phases separated by an explicit barrier:
 *
 * 1. `collecting` — events emitted by the source before its initial `ready`
 *    are merged into a per-path baseline instead of being delivered.
 * 2. `streaming` — once the source emits `ready`, the merged baseline is
 *    published exactly once (a single `baseline` event followed by `ready`),
 *    and every later source event is delivered as an incremental event.
 *
 * The barrier is a strict order: a record is attributed to the baseline iff
 * it was queued before the source's `ready` record. Nothing is delivered
 * twice and nothing is both baseline and increment. If the source errors or
 * the adapter is closed before `ready`, no (partial) baseline is published.
 */
import { EventEmitter } from 'node:events';
import type { Stats } from 'node:fs';
import { watch } from './index.js';
import { type ChokidarOptions, type EmitArgs, EVENTS as EV, type Path } from './runtime.js';

/** Events the adapter forwards as increments (and merges into the baseline). */
export type StartupIncrementEvent = 'add' | 'addDir' | 'change' | 'unlink' | 'unlinkDir';

/** Lifecycle of a StartupAdapter. */
export type StartupState =
  /** Buffering the initial scan; nothing has been delivered yet. */
  | 'collecting'
  /** Baseline was published; events stream through as increments. */
  | 'streaming'
  /** close() ran (possibly before ready); terminal, no baseline is published. */
  | 'closed'
  /** Startup failed (buffer overflow or source error before ready); terminal. */
  | 'failed';

/** One entry of the published baseline: the final state of a path at ready. */
export type BaselineEntry = {
  /** `add` for files, `addDir` for directories. */
  event: 'add' | 'addDir';
  path: Path;
  stats?: Stats;
};

/** A record in the adapter's ordered event queue. Exposed as a test seam. */
export type StartupQueueRecord =
  | { kind: 'event'; event: StartupIncrementEvent; args: EmitArgs }
  | { kind: 'ready' }
  | { kind: 'error'; error: Error };

/** Optional hooks, mainly a test seam for controlling queue processing. */
export interface StartupHooks {
  /**
   * Awaited before each queued record is processed. Injecting an async hook
   * switches the queue to asynchronous processing and lets a test pause the
   * queue at an exact position (e.g. right at the `ready` barrier).
   */
  beforeRecord?: (record: StartupQueueRecord) => void | Promise<void>;
}

export interface StartupAdapterOptions {
  /**
   * Maximum number of distinct paths buffered while collecting the baseline.
   * Exceeding the limit fails startup: an `error` is emitted, the source
   * watcher is closed and no baseline is published. Defaults to
   * DEFAULT_MAX_BUFFERED_PATHS.
   */
  maxBufferedPaths?: number;
  hooks?: StartupHooks;
}

/** Default cap on buffered baseline paths. */
export const DEFAULT_MAX_BUFFERED_PATHS = 1_000_000;

/** `code` of the error emitted when the baseline buffer overflows. */
export const STARTUP_BUFFER_OVERFLOW = 'CHOKIDAR_STARTUP_BUFFER_OVERFLOW';

/** Error emitted (and startup aborted) when the baseline buffer overflows. */
export class StartupBufferOverflowError extends Error {
  code: string = STARTUP_BUFFER_OVERFLOW;
  readonly limit: number;
  constructor(limit: number) {
    super(`Startup baseline buffer exceeded the limit of ${limit} paths`);
    this.name = 'StartupBufferOverflowError';
    this.limit = limit;
  }
}

/**
 * Minimal source the adapter needs. `FSWatcher` satisfies this structurally;
 * tests can substitute a fake emitter with a `close()` method.
 */
export interface StartupSource {
  on(event: string, listener: (...args: any[]) => void): this;
  off(event: string, listener: (...args: any[]) => void): this;
  close(): Promise<void>;
}

export interface StartupAdapterEventMap {
  baseline: [entries: readonly BaselineEntry[]];
  ready: [];
  error: [error: Error];
  all: [event: StartupIncrementEvent, ...EmitArgs];
  add: EmitArgs;
  addDir: EmitArgs;
  change: EmitArgs;
  unlink: EmitArgs;
  unlinkDir: EmitArgs;
}

const INCREMENT_EVENTS: readonly StartupIncrementEvent[] = [
  EV.ADD,
  EV.ADD_DIR,
  EV.CHANGE,
  EV.UNLINK,
  EV.UNLINK_DIR,
];

/**
 * Startup adapter over a chokidar-compatible watcher. See module docs for the
 * state machine. Emits `baseline`, `ready`, incremental `add` / `addDir` /
 * `change` / `unlink` / `unlinkDir` (plus `all`), and `error`.
 */
export class StartupAdapter extends EventEmitter<StartupAdapterEventMap> {
  private source: StartupSource;
  private readonly maxBufferedPaths: number;
  private readonly hooks: StartupHooks;
  private buffer: Map<string, BaselineEntry> = new Map();
  private queue: StartupQueueRecord[] = [];
  private pumping: boolean = false;
  private idleResolvers: Array<() => void> = [];
  private stateValue: StartupState = 'collecting';
  private baselineSnapshot?: readonly BaselineEntry[];
  private closePromise?: Promise<void>;
  private sourceListeners: Array<[string, (...args: any[]) => void]> = [];

  constructor(source: StartupSource, options: StartupAdapterOptions = {}) {
    super();
    const max = options.maxBufferedPaths ?? DEFAULT_MAX_BUFFERED_PATHS;
    if (!Number.isSafeInteger(max) || max <= 0) {
      throw new TypeError('maxBufferedPaths must be a positive safe integer');
    }
    this.source = source;
    this.maxBufferedPaths = max;
    this.hooks = options.hooks ?? {};
    for (const event of INCREMENT_EVENTS) {
      this.attach(event, (...args: any[]) => {
        this.enqueue({ kind: 'event', event, args: args as EmitArgs });
      });
    }
    this.attach(EV.READY, () => this.enqueue({ kind: 'ready' }));
    this.attach(EV.ERROR, (error: unknown) => {
      this.enqueue({
        kind: 'error',
        error: error instanceof Error ? error : new Error(String(error)),
      });
    });
  }

  /** Current lifecycle state. */
  get state(): StartupState {
    return this.stateValue;
  }

  /** Whether the adapter is terminal (`closed` or `failed`). */
  get closed(): boolean {
    return this.stateValue === 'closed' || this.stateValue === 'failed';
  }

  /** The published baseline, or `undefined` before `ready` / after a failure. */
  get baseline(): readonly BaselineEntry[] | undefined {
    return this.baselineSnapshot;
  }

  /** Resolves once every record queued so far has been processed. */
  whenIdle(): Promise<void> {
    if (!this.pumping && this.queue.length === 0) return Promise.resolve();
    return new Promise((resolve) => {
      this.idleResolvers.push(resolve);
    });
  }

  /**
   * Detaches from the source and closes it. Idempotent. Closing before
   * `ready` is an early exit: the buffered baseline is discarded and never
   * published.
   */
  close(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    this.stateValue = 'closed';
    this.buffer.clear();
    this.queue.length = 0;
    this.detach();
    this.closePromise = Promise.resolve(this.source.close());
    return this.closePromise;
  }

  private attach(event: string, listener: (...args: any[]) => void): void {
    this.sourceListeners.push([event, listener]);
    this.source.on(event, listener);
  }

  private detach(): void {
    for (const [event, listener] of this.sourceListeners) this.source.off(event, listener);
    this.sourceListeners = [];
  }

  private enqueue(record: StartupQueueRecord): void {
    if (this.closed) return;
    this.queue.push(record);
    if (this.hooks.beforeRecord) {
      void this.pumpAsync();
    } else {
      this.drainSync();
    }
  }

  private notifyIdle(): void {
    if (this.queue.length !== 0) return;
    const resolvers = this.idleResolvers.splice(0);
    for (const resolve of resolvers) resolve();
  }

  private drainSync(): void {
    if (this.pumping) return;
    this.pumping = true;
    try {
      let record: StartupQueueRecord | undefined;
      while ((record = this.queue.shift()) !== undefined) {
        if (this.closed) {
          this.queue.length = 0;
          break;
        }
        this.process(record);
      }
    } finally {
      this.pumping = false;
      this.notifyIdle();
    }
  }

  private async pumpAsync(): Promise<void> {
    if (this.pumping) return;
    this.pumping = true;
    try {
      let record: StartupQueueRecord | undefined;
      while ((record = this.queue.shift()) !== undefined) {
        if (this.closed) {
          this.queue.length = 0;
          break;
        }
        try {
          await this.hooks.beforeRecord!(record);
        } catch (error) {
          record = {
            kind: 'error',
            error: error instanceof Error ? error : new Error(String(error)),
          };
        }
        if (this.closed) {
          this.queue.length = 0;
          break;
        }
        this.process(record);
      }
    } finally {
      this.pumping = false;
      this.notifyIdle();
    }
  }

  private process(record: StartupQueueRecord): void {
    switch (record.kind) {
      case 'ready':
        if (this.stateValue === 'collecting') this.publishBaseline();
        break;
      case 'error':
        if (this.stateValue === 'collecting') this.fail(record.error);
        else if (this.stateValue === 'streaming') this.emit(EV.ERROR, record.error);
        break;
      case 'event':
        if (this.stateValue === 'collecting') this.mergeIntoBaseline(record.event, record.args);
        else if (this.stateValue === 'streaming') this.emitIncrement(record.event, record.args);
        break;
    }
  }

  private mergeIntoBaseline(event: StartupIncrementEvent, args: EmitArgs): void {
    const [path, stats] = args;
    switch (event) {
      case EV.ADD:
      case EV.ADD_DIR:
        this.setBaselineEntry({ event, path, stats });
        break;
      case EV.CHANGE: {
        // A change before ready only refines the baseline: the path exists.
        const existing = this.buffer.get(path);
        if (existing) {
          if (stats !== undefined) existing.stats = stats;
        } else {
          this.setBaselineEntry({ event: EV.ADD, path, stats });
        }
        break;
      }
      case EV.UNLINK:
      case EV.UNLINK_DIR:
        // Added and removed during the initial scan: absent from the baseline.
        this.buffer.delete(path);
        break;
    }
  }

  private setBaselineEntry(entry: BaselineEntry): void {
    if (!this.buffer.has(entry.path) && this.buffer.size >= this.maxBufferedPaths) {
      this.fail(new StartupBufferOverflowError(this.maxBufferedPaths));
      return;
    }
    this.buffer.set(entry.path, entry);
  }

  private publishBaseline(): void {
    const entries: readonly BaselineEntry[] = Object.freeze(
      [...this.buffer.values()].map((entry) => Object.freeze({ ...entry }))
    );
    this.buffer.clear();
    this.stateValue = 'streaming';
    this.baselineSnapshot = entries;
    this.emit('baseline', entries);
    this.emit(EV.READY);
  }

  private emitIncrement(event: StartupIncrementEvent, args: EmitArgs): void {
    this.emit(event, ...args);
    this.emit(EV.ALL, event, ...args);
  }

  private fail(error: Error): void {
    if (this.closed) return;
    this.stateValue = 'failed';
    this.buffer.clear();
    this.queue.length = 0;
    this.detach();
    this.closePromise = Promise.resolve().then(() => this.source.close());
    this.emit(EV.ERROR, error);
  }
}

/**
 * Watches `paths` with chokidar and wraps the watcher in a StartupAdapter:
 * the initial scan is published as one `baseline` event at `ready`, later
 * changes stream as increments.
 * @param paths file / directory paths
 * @param options regular chokidar options
 * @param startupOptions adapter options (buffer limit, test hooks)
 */
export function watchStartup(
  paths: string | string[],
  options: ChokidarOptions = {},
  startupOptions: StartupAdapterOptions = {}
): StartupAdapter {
  return new StartupAdapter(watch(paths, options), startupOptions);
}

const defaultExport: { watchStartup: typeof watchStartup; StartupAdapter: typeof StartupAdapter } =
  { watchStartup, StartupAdapter };
export default defaultExport;
