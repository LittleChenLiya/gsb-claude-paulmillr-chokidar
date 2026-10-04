/*! chokidar - MIT License (c) 2012 Paul Miller (paulmillr.com) */
import * as sp from 'node:path';
import {
  EVENTS,
  logicalPathKey,
  type EmitArgs,
  type EventName,
  type FSWInstanceOptions,
  type Path,
} from './runtime.js';

/**
 * Startup state machine.
 *
 * ```text
 * buffering ──seal()──▶ sealed ──goLive()──▶ live
 *     │                   │
 *     ├──fail()──▶ failed └── (increments pass through)
 *     └──close()──▶ closed
 * ```
 *
 * While `buffering`, public events for a path are merged into a single
 * baseline entry. `seal()` is the barrier: events committed before it belong
 * to the baseline, events committed after it are ordinary increments. The
 * baseline is published exactly once, synchronously, before `ready`.
 */
export type StartupPhase = 'buffering' | 'sealed' | 'live' | 'failed' | 'closed';

export type BaselineEntry = {
  event: EventName;
  args: EmitArgs;
};

export type DeliverEvent = (event: EventName, args: EmitArgs) => void;

export const DEFAULT_BASELINE_LIMIT = 1024;

/**
 * Collects pre-ready `add` / `addDir` emissions into one baseline and
 * publishes them once at the `ready` barrier, then switches to incremental
 * delivery. Add / change / unlink of the same path during the initial scan
 * merge into the final baseline state.
 */
export class StartupAdapter {
  readonly limit: number;
  private phase: StartupPhase = 'buffering';
  private readonly entries = new Map<string, BaselineEntry>();
  private readonly options: FSWInstanceOptions;
  onFail?: (error: Error) => void;

  constructor(options: FSWInstanceOptions, limit: number = DEFAULT_BASELINE_LIMIT) {
    this.options = options;
    this.limit = limit;
  }

  get state(): StartupPhase {
    return this.phase;
  }

  /** True while the baseline is still being collected. */
  get buffering(): boolean {
    return this.phase === 'buffering';
  }

  get size(): number {
    return this.entries.size;
  }

  private keyOf(args: EmitArgs): string {
    const presented = args[0] as Path;
    const absolute = this.options.cwd ? sp.resolve(this.options.cwd, presented) : presented;
    return logicalPathKey(absolute);
  }

  /**
   * Commit one public event. During buffering it is merged into the baseline;
   * after the barrier it is delivered as an increment. `error` always
   * delivers immediately so failures are never held behind the baseline.
   */
  publish(event: EventName, args: EmitArgs, deliver: DeliverEvent): void {
    if (this.phase === 'live' || this.phase === 'sealed') {
      deliver(event, args);
      return;
    }
    if (this.phase !== 'buffering') return;
    if (event === EVENTS.ERROR) {
      deliver(event, args);
      return;
    }

    const key = this.keyOf(args);
    if (event === EVENTS.ADD || event === EVENTS.ADD_DIR) {
      const existing = this.entries.get(key);
      if (existing) {
        // Final state wins: a re-added path keeps the later event kind/stats.
        existing.event = event;
        existing.args = args;
        return;
      }
      if (this.entries.size >= this.limit) {
        this.fail(
          new Error(
            `Startup baseline buffer exceeded its limit of ${this.limit} distinct paths`
          )
        );
        return;
      }
      this.entries.set(key, { event, args });
      return;
    }
    if (event === EVENTS.CHANGE) {
      // A change during the scan only refreshes the baseline stats.
      const existing = this.entries.get(key);
      if (existing) existing.args = args;
      return;
    }
    if (event === EVENTS.UNLINK || event === EVENTS.UNLINK_DIR) {
      this.entries.delete(key);
      return;
    }
    // `ready` / `all` / `raw` never reach this point; deliver defensively.
    deliver(event, args);
  }

  /**
   * The barrier. Freezes the baseline and returns its entries in insertion
   * (scan) order. Events committed after this call are increments.
   */
  seal(): BaselineEntry[] {
    if (this.phase !== 'buffering') return [];
    this.phase = 'sealed';
    return [...this.entries.values()];
  }

  /** Opens incremental delivery after the baseline has been published. */
  goLive(): void {
    if (this.phase === 'sealed') this.phase = 'live';
  }

  /** Terminal startup failure: drop the baseline and report once. */
  fail(error: Error): void {
    if (this.phase === 'failed' || this.phase === 'closed') return;
    this.phase = 'failed';
    this.entries.clear();
    this.onFail?.(error);
  }

  /** Early close: no baseline is ever published. */
  close(): void {
    if (this.phase === 'closed') return;
    this.phase = 'closed';
    this.entries.clear();
  }
}
