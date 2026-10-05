/**
 * Explicit rename correlator.
 *
 * Synthesizes rename events only from a *unique* device+file-ID match
 * between a pending unlink and a pending add. It never guesses from name
 * similarity. Identities of deleted paths come from a cache populated while
 * the path was still alive; identities of added paths may arrive late through
 * an asynchronous stat, which is validated against the path generation so a
 * stale stat cannot overwrite a newer generation of the same path.
 *
 * Hard links and any multi-candidate constellation are marked uncertain
 * instead of being merged. Entries whose identity is unknown are never
 * force-merged. Every emitted event keeps its original source(s) so the
 * backend trigger that produced it stays traceable, and every pending entry
 * is flushed after an injected-clock wait window so no event is ever dropped.
 */
import type { Stats } from 'node:fs';
import {
  EVENTS,
  type Path,
  type Scheduler,
  type SchedulerTimer,
  systemScheduler,
} from './runtime.js';

const EV = EVENTS;

/** Reliable file identity: device + file ID (ino). Never derived from names. */
export type FileIdentity = { dev: number; ino: number };

/** Traceable origin of a correlated event: the raw observation that fed it. */
export type RenameEventSource = {
  event: 'add' | 'unlink';
  path: Path;
  /** Sequence number of the backend trigger that produced this observation. */
  sequence: number;
  /** Timestamp reported by the backend for the original observation. */
  observedAt: number;
};

export type CorrelatedRename = {
  event: 'rename';
  /** New path (the add side). */
  path: Path;
  /** Old path (the unlink side). */
  oldPath: Path;
  stats?: Stats;
  uncertain: false;
  /** Exactly the two original observations this rename was synthesized from. */
  sources: [RenameEventSource, RenameEventSource];
};

export type CorrelatedPassthrough = {
  event: 'add' | 'unlink';
  path: Path;
  stats?: Stats;
  /** True when a hard-link / multi-candidate clash prevented correlation. */
  uncertain: boolean;
  /** The single original observation; kept so nothing is dropped untraceably. */
  sources: [RenameEventSource];
};

export type CorrelatedEvent = CorrelatedRename | CorrelatedPassthrough;

export type RenameCorrelatorOptions = {
  /** Wait window in milliseconds before an unmatched entry is flushed. */
  windowMs?: number;
  /** Injected clock/scheduler; defaults to the system clock. */
  scheduler?: Scheduler;
  /** Sink for correlated (or faithfully forwarded) events. */
  emit: (event: CorrelatedEvent) => void;
};

type PendingEntry = {
  event: 'add' | 'unlink';
  path: Path;
  generation: number;
  identity?: FileIdentity;
  stats?: Stats;
  source: RenameEventSource;
  uncertain: boolean;
  timer?: SchedulerTimer;
  pair?: PendingPair;
};

/**
 * A tentative 1:1 identity match. The rename is only emitted once the pair
 * survives a full wait window, so a late hard-link candidate can still break
 * the pair and demote both sides to uncertain passthrough events.
 */
type PendingPair = {
  add: PendingEntry;
  unlink: PendingEntry;
  timer?: SchedulerTimer;
};

const identityKey = (identity: FileIdentity): string =>
  String(identity.dev) + ':' + String(identity.ino);

const sameIdentity = (a: FileIdentity | undefined, b: FileIdentity | undefined): boolean =>
  a !== undefined && b !== undefined && a.dev === b.dev && a.ino === b.ino;

export class RenameCorrelator {
  private readonly windowMs: number;
  private readonly scheduler: Scheduler;
  private readonly emit: (event: CorrelatedEvent) => void;
  /** Identity cache for live paths, used to identify later deletions. */
  private readonly knownIdentities = new Map<Path, FileIdentity>();
  /** Current generation per path; bumped on every add/unlink observation. */
  private readonly generations = new Map<Path, number>();
  private readonly pendingAdds = new Set<PendingEntry>();
  private readonly pendingUnlinks = new Set<PendingEntry>();
  private sequence = 0;

  constructor(options: RenameCorrelatorOptions) {
    this.windowMs = options.windowMs ?? 100;
    this.scheduler = options.scheduler ?? systemScheduler;
    this.emit = options.emit;
  }

  private nextSequence(): number {
    return ++this.sequence;
  }

  private generationOf(path: Path): number {
    return this.generations.get(path) ?? 0;
  }

  private bumpGeneration(path: Path): number {
    const generation = this.generationOf(path) + 1;
    this.generations.set(path, generation);
    return generation;
  }

  /**
   * Record the identity of a live path (from an add/change stat). This
   * cache is what later identifies the path when it is unlinked and can no
   * longer be statted.
   */
  noteIdentity(path: Path, stats: Stats): void {
    this.knownIdentities.set(path, { dev: stats.dev, ino: stats.ino });
  }

  /** Forget a live-path identity (e.g. after the entry was replaced). */
  forgetIdentity(path: Path): void {
    this.knownIdentities.delete(path);
  }

  /**
   * Observe an add. The identity may be unknown yet; call resolveAddStat
   * when the asynchronous stat arrives.
   */
  add(path: Path, stats?: Stats, observedAt?: number): RenameEventSource {
    const generation = this.bumpGeneration(path);
    const source: RenameEventSource = {
      event: EV.ADD,
      path,
      sequence: this.nextSequence(),
      observedAt: observedAt ?? this.scheduler.now(),
    };
    const entry: PendingEntry = {
      event: EV.ADD,
      path,
      generation,
      source,
      uncertain: false,
    };
    if (stats) {
      entry.stats = stats;
      entry.identity = { dev: stats.dev, ino: stats.ino };
      this.noteIdentity(path, stats);
    }
    this.pendingAdds.add(entry);
    this.arm(entry, this.pendingAdds);
    if (entry.identity) this.tryCorrelate(entry.identity);
    return source;
  }

  /**
   * Observe an unlink. The identity is taken from the cache populated while
   * the path was alive; without a cached identity it stays unknown and will
   * never be force-merged.
   */
  unlink(path: Path, observedAt?: number): RenameEventSource {
    const generation = this.bumpGeneration(path);
    const source: RenameEventSource = {
      event: EV.UNLINK,
      path,
      sequence: this.nextSequence(),
      observedAt: observedAt ?? this.scheduler.now(),
    };
    const identity = this.knownIdentities.get(path);
    this.knownIdentities.delete(path);
    const entry: PendingEntry = {
      event: EV.UNLINK,
      path,
      generation,
      identity,
      source,
      uncertain: false,
    };
    this.pendingUnlinks.add(entry);
    this.arm(entry, this.pendingUnlinks);
    if (identity) this.tryCorrelate(identity);
    return source;
  }

  /**
   * Apply a late asynchronous stat for a pending add. The stat is only
   * honored when the generation still matches the current generation of the
   * path; a stat that was in flight while the path was unlinked and re-added
   * belongs to an older generation and is discarded.
   */
  resolveAddStat(path: Path, generation: number, stats: Stats): boolean {
    if (generation !== this.generationOf(path)) return false;
    for (const entry of this.pendingAdds) {
      if (entry.path !== path || entry.generation !== generation) continue;
      entry.stats = stats;
      entry.identity = { dev: stats.dev, ino: stats.ino };
      this.noteIdentity(path, stats);
      this.tryCorrelate(entry.identity);
      return true;
    }
    return false;
  }

  /** Current generation token for a path, for pairing with resolveAddStat. */
  captureGeneration(path: Path): number {
    return this.generationOf(path);
  }

  /**
   * Correlate strictly on unique identity. Exactly one pending add and one
   * pending unlink with the same identity form a tentative pair whose rename
   * is emitted after the wait window. If more than one pending entry on
   * either side shares the identity (hard links, reused inodes, duplicates),
   * every candidate is only marked uncertain, any tentative pair is broken,
   * and all of them flush independently.
   */
  private tryCorrelate(identity: FileIdentity): void {
    const adds = [...this.pendingAdds].filter((entry) => sameIdentity(entry.identity, identity));
    const unlinks = [...this.pendingUnlinks].filter((entry) =>
      sameIdentity(entry.identity, identity)
    );
    if (adds.length === 0 || unlinks.length === 0) return;
    if (adds.length > 1 || unlinks.length > 1) {
      for (const entry of [...adds, ...unlinks]) {
        entry.uncertain = true;
        this.breakPair(entry);
      }
      return;
    }
    const [addEntry] = adds;
    const [unlinkEntry] = unlinks;
    if (addEntry.uncertain || unlinkEntry.uncertain) return;
    if (addEntry.pair || unlinkEntry.pair) return;
    // Tentative pair: stop the individual flush timers and let the pair
    // timer emit the rename once a full window passed without new candidates.
    this.scheduler.clearTimeout(addEntry.timer);
    addEntry.timer = undefined;
    this.scheduler.clearTimeout(unlinkEntry.timer);
    unlinkEntry.timer = undefined;
    const pair: PendingPair = { add: addEntry, unlink: unlinkEntry };
    addEntry.pair = pair;
    unlinkEntry.pair = pair;
    pair.timer = this.scheduler.setTimeout(() => this.flushPair(pair), this.windowMs);
  }

  /** Break a tentative pair; both members flush on their own again. */
  private breakPair(entry: PendingEntry): void {
    const pair = entry.pair;
    if (!pair) return;
    this.scheduler.clearTimeout(pair.timer);
    pair.timer = undefined;
    pair.add.pair = undefined;
    pair.unlink.pair = undefined;
    this.arm(pair.add, this.pendingAdds);
    this.arm(pair.unlink, this.pendingUnlinks);
  }

  /** Emit a rename for a pair that survived its wait window. */
  private flushPair(pair: PendingPair): void {
    pair.timer = undefined;
    pair.add.pair = undefined;
    pair.unlink.pair = undefined;
    if (!this.pendingAdds.delete(pair.add) || !this.pendingUnlinks.delete(pair.unlink)) return;
    this.emit({
      event: 'rename',
      path: pair.add.path,
      oldPath: pair.unlink.path,
      stats: pair.add.stats,
      uncertain: false,
      sources: [pair.unlink.source, pair.add.source],
    });
  }

  private arm(entry: PendingEntry, pool: Set<PendingEntry>): void {
    entry.timer = this.scheduler.setTimeout(() => this.flush(entry, pool), this.windowMs);
  }

  /** Flush an unmatched entry after the wait window; nothing is dropped. */
  private flush(entry: PendingEntry, pool: Set<PendingEntry>): void {
    if (!pool.delete(entry)) return;
    entry.timer = undefined;
    this.emit({
      event: entry.event,
      path: entry.path,
      stats: entry.stats,
      uncertain: entry.uncertain,
      sources: [entry.source],
    });
  }

  /** Test/diagnostic view of still-pending observations. */
  get pending(): { adds: number; unlinks: number } {
    return { adds: this.pendingAdds.size, unlinks: this.pendingUnlinks.size };
  }
}

export const identityOf: (stats: Stats) => FileIdentity = (stats) => ({
  dev: stats.dev,
  ino: stats.ino,
});
export const fileIdentityKey: (identity: FileIdentity) => string = identityKey;
