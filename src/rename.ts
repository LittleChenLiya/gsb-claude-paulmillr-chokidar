import type { Stats } from 'node:fs';
import {
  EVENTS,
  type EventName,
  type Path,
  type Scheduler,
  type SchedulerTimer,
  logicalPathKey,
} from './runtime.js';

/**
 * Explicit rename correlator.
 *
 * A rename is synthesised only from an `unlink` and an `add` that share a
 * reliable device + file identity (`dev:ino`) and match one-to-one. The
 * correlator never guesses by name similarity: an event whose identity is
 * unknown, or whose identity has more than one candidate, is released as the
 * original `unlink`/`add` so no observation is ever dropped.
 *
 * Deletion identities are read from the caller's cached tree observation, not
 * from a fresh stat (a deleted path cannot be re-stat'd). An add that arrives
 * without stats waits for a late stat; the path generation recorded at wait
 * time is re-checked when the stat lands, so a stale stat cannot overwrite a
 * newer generation.
 *
 * The waiting window is driven by an injected {@link Scheduler}, making the
 * whole correlation deterministic under a virtual clock. Matching is deferred
 * until the window closes so that additional candidates (hard links, multiple
 * unlinks/adds) can arrive and keep the match from being unique.
 */

export const RENAME_EVENT = 'rename';

export type FileIdentity = {
  dev: number;
  ino: number;
};

export function identityKey(identity: FileIdentity): string {
  return `${identity.dev}:${identity.ino}`;
}

/** Extracts a reliable identity from stats, or undefined when unavailable. */
export function statsIdentity(stats: Stats | undefined): FileIdentity | undefined {
  if (!stats) return undefined;
  const { dev, ino } = stats;
  if (typeof dev !== 'number' || typeof ino !== 'number') return undefined;
  if (!Number.isFinite(dev) || !Number.isFinite(ino)) return undefined;
  return { dev, ino };
}

export type RenameSource = {
  event: EventName;
  path: Path;
  identity: FileIdentity;
  observedAt: number;
  stats?: Stats;
  generation?: number;
};

export type RenameCorrelation = {
  kind: typeof RENAME_EVENT;
  identity: FileIdentity;
  oldPath: Path;
  newPath: Path;
  stats?: Stats;
  observedAt: number;
  sources: {
    unlink: RenameSource;
    add: RenameSource;
  };
};

type PendingEntry = {
  source: RenameSource;
  timer: SchedulerTimer;
  identityKey: string;
};

export type RenameCorrelatorOptions = {
  scheduler: Scheduler;
  /** Waiting window in milliseconds. Defaults to 100. */
  window?: number;
  /** Lifecycle gate; when false the correlator drops observations. */
  isActive?: () => boolean;
  /** Path-generation gate for late stats. */
  isPathGenerationActive?: (path: Path, generation: number) => boolean;
  /**
   * Hard-link guard: true when `identity` is currently observed at a path
   * other than `excludePath`.
   */
  identityExistsElsewhere?: (identity: FileIdentity, excludePath: Path) => boolean;
  onRename?: (correlation: RenameCorrelation) => void;
  onUnlink?: (path: Path, source: RenameSource) => void;
  onAdd?: (path: Path, stats: Stats | undefined, source: RenameSource) => void;
  onUncertain?: (
    identity: FileIdentity,
    reason: 'multiple' | 'hardlink',
    paths: Path[]
  ) => void;
};

const DEFAULT_WINDOW = 100;

export class RenameCorrelator {
  readonly scheduler: Scheduler;
  readonly window: number;
  private readonly isActive: () => boolean;
  private readonly isPathGenerationActive?: (path: Path, generation: number) => boolean;
  private readonly identityExistsElsewhere?: (
    identity: FileIdentity,
    excludePath: Path
  ) => boolean;
  private readonly onRename?: (correlation: RenameCorrelation) => void;
  private readonly onUnlink?: (path: Path, source: RenameSource) => void;
  private readonly onAdd?: (path: Path, stats: Stats | undefined, source: RenameSource) => void;
  private readonly onUncertain?: (
    identity: FileIdentity,
    reason: 'multiple' | 'hardlink',
    paths: Path[]
  ) => void;

  /** identityKey -> pending unlinks (deferred until the window closes). */
  private readonly pendingUnlinks: Map<string, PendingEntry[]> = new Map();
  /** identityKey -> pending adds (deferred until the window closes). */
  private readonly pendingAdds: Map<string, PendingEntry[]> = new Map();
  /** pathKey -> pending add waiting for a late stat. */
  private readonly addsByPath: Map<string, PendingEntry> = new Map();
  /** identity keys that cannot be uniquely matched. */
  private readonly uncertain: Set<string> = new Set();
  /** pathKey -> latest generation (bumped on every noteAdd for that path). */
  private readonly pathGenerations: Map<string, number> = new Map();
  private closed = false;

  constructor(options: RenameCorrelatorOptions) {
    this.scheduler = options.scheduler;
    this.window = options.window ?? DEFAULT_WINDOW;
    this.isActive = options.isActive ?? (() => true);
    this.isPathGenerationActive = options.isPathGenerationActive;
    this.identityExistsElsewhere = options.identityExistsElsewhere;
    this.onRename = options.onRename;
    this.onUnlink = options.onUnlink;
    this.onAdd = options.onAdd;
    this.onUncertain = options.onUncertain;
  }

  private now(): number {
    return this.scheduler.now();
  }

  private makeTimer(callback: () => void): SchedulerTimer {
    return this.scheduler.setTimeout(callback, this.window);
  }

  private clearTimer(timer: SchedulerTimer | undefined): void {
    if (timer) this.scheduler.clearTimeout(timer);
  }

  /**
   * Record an unlink observation.
   *
   * The identity is supplied by the caller from the previous tree cache; a
   * deleted path cannot be re-stat'd. When no identity is available the
   * event is released immediately as an unlink: an unknown identity is never
   * forcibly merged.
   */
  noteUnlink(
    path: Path,
    identity?: FileIdentity,
    event: EventName = EVENTS.UNLINK
  ): void {
    if (this.closed || !this.isActive()) return;
    const observedAt = this.now();
    if (!identity) {
      this.onUnlink?.(path, { event, path, identity: UNKNOWN_IDENTITY, observedAt });
      return;
    }
    const key = identityKey(identity);
    if (this.uncertain.has(key)) {
      this.onUnlink?.(path, { event, path, identity, observedAt });
      return;
    }
    const entry: PendingEntry = {
      source: { event, path, identity, observedAt },
      timer: this.makeTimer(() => this.checkMatch(key)),
      identityKey: key,
    };
    const existing = this.pendingUnlinks.get(key) ?? [];
    existing.push(entry);
    this.pendingUnlinks.set(key, existing);
  }

  /**
   * Record an add observation.
   *
   * The identity is taken from stats when available. When stats are absent
   * the add waits for a late stat ({@link noteStat}); the path generation is
   * recorded so a stale stat cannot overwrite a newer generation.
   */
  noteAdd(
    path: Path,
    stats?: Stats,
    identity?: FileIdentity,
    event: EventName = EVENTS.ADD
  ): void {
    if (this.closed || !this.isActive()) return;
    const observedAt = this.now();
    // A newer add supersedes any pending stat-less add for the same path.
    this.supersedePath(path);
    const resolvedIdentity = identity ?? statsIdentity(stats);
    if (!resolvedIdentity) {
      this.waitForStat(path, stats, event, observedAt);
      return;
    }
    this.applyAdd(path, stats, resolvedIdentity, event, observedAt);
  }

  /**
   * Deliver a late stat for a previously stat-less add.
   *
   * The stat is applied only when its recorded path generation is still the
   * latest for that path; otherwise it is discarded so a late old stat cannot
   * overwrite a new generation.
   */
  noteStat(path: Path, stats: Stats): void {
    if (this.closed || !this.isActive()) return;
    const pathKey = logicalPathKey(path);
    const entry = this.addsByPath.get(pathKey);
    if (!entry) return;
    // Verify the path generation: a newer add/unwatch supersedes this stat.
    const latestGeneration = this.pathGenerations.get(pathKey);
    if (entry.source.generation !== latestGeneration) {
      this.clearTimer(entry.timer);
      this.addsByPath.delete(pathKey);
      return;
    }
    if (
      this.isPathGenerationActive &&
      !this.isPathGenerationActive(path, entry.source.generation ?? 0)
    ) {
      this.clearTimer(entry.timer);
      this.addsByPath.delete(pathKey);
      return;
    }
    this.clearTimer(entry.timer);
    this.addsByPath.delete(pathKey);
    const identity = statsIdentity(stats);
    if (!identity) {
      this.onAdd?.(path, stats, entry.source);
      return;
    }
    this.applyAdd(path, stats, identity, entry.source.event, entry.source.observedAt);
  }

  /**
   * Bump the path generation and clear any pending stat-less add for `path`.
   * The superseded add is replaced, not released: the newer observation is the
   * authoritative one.
   */
  private supersedePath(path: Path): void {
    const pathKey = logicalPathKey(path);
    const existing = this.addsByPath.get(pathKey);
    if (existing) {
      this.clearTimer(existing.timer);
      this.addsByPath.delete(pathKey);
    }
    const generation = (this.pathGenerations.get(pathKey) ?? 0) + 1;
    this.pathGenerations.set(pathKey, generation);
  }

  private waitForStat(
    path: Path,
    stats: Stats | undefined,
    event: EventName,
    observedAt: number
  ): void {
    const pathKey = logicalPathKey(path);
    const generation = this.pathGenerations.get(pathKey) ?? 1;
    const source: RenameSource = {
      event,
      path,
      identity: UNKNOWN_IDENTITY,
      observedAt,
      stats,
      generation,
    };
    const entry: PendingEntry = {
      source,
      timer: this.makeTimer(() => this.expireAddByPath(pathKey, entry)),
      identityKey: '',
    };
    this.addsByPath.set(pathKey, entry);
  }

  private applyAdd(
    path: Path,
    stats: Stats | undefined,
    identity: FileIdentity,
    event: EventName,
    observedAt: number
  ): void {
    const key = identityKey(identity);
    if (this.uncertain.has(key)) {
      this.onAdd?.(path, stats, { event, path, identity, observedAt });
      return;
    }
    // Hard-link guard: identity observed at another current path.
    if (this.identityExistsElsewhere && this.identityExistsElsewhere(identity, path)) {
      this.markUncertain(identity, 'hardlink', [path]);
      this.onAdd?.(path, stats, { event, path, identity, observedAt });
      return;
    }
    const entry: PendingEntry = {
      source: { event, path, identity, observedAt, stats },
      timer: this.makeTimer(() => this.checkMatch(key)),
      identityKey: key,
    };
    const existing = this.pendingAdds.get(key) ?? [];
    existing.push(entry);
    this.pendingAdds.set(key, existing);
  }

  /**
   * Re-examine all pending entries for an identity once the window closes.
   * A rename is synthesised only for a unique one-to-one match; multiple
   * candidates or hard links are marked uncertain and released as independent
   * events.
   */
  private checkMatch(key: string): void {
    if (this.uncertain.has(key)) return;
    const unlinks = this.pendingUnlinks.get(key) ?? [];
    const adds = this.pendingAdds.get(key) ?? [];
    if (unlinks.length === 0 && adds.length === 0) return;

    if (unlinks.length === 1 && adds.length === 1) {
      const unlink = unlinks[0];
      const add = adds[0];
      // A rename needs two distinct paths.
      if (unlink.source.path === add.source.path) {
        this.releaseAll(key, unlinks, adds);
        return;
      }
      // Re-check the hard-link guard now that both sides are present.
      if (
        this.identityExistsElsewhere &&
        this.identityExistsElsewhere(unlink.source.identity, add.source.path)
      ) {
        this.markUncertain(unlink.source.identity, 'hardlink', [
          unlink.source.path,
          add.source.path,
        ]);
        this.releaseAll(key, unlinks, adds);
        return;
      }
      this.clearAll(key, unlinks, adds);
      const correlation: RenameCorrelation = {
        kind: RENAME_EVENT,
        identity: unlink.source.identity,
        oldPath: unlink.source.path,
        newPath: add.source.path,
        stats: add.source.stats,
        observedAt: Math.max(unlink.source.observedAt, add.source.observedAt),
        sources: { unlink: unlink.source, add: add.source },
      };
      this.onRename?.(correlation);
      return;
    }

    if (unlinks.length > 1 || adds.length > 1) {
      // Multiple candidates: the match is not unique.
      const identity = unlinks[0]?.source.identity ?? adds[0]?.source.identity;
      if (identity) {
        this.markUncertain(identity, 'multiple', [
          ...unlinks.map((e) => e.source.path),
          ...adds.map((e) => e.source.path),
        ]);
      }
      this.releaseAll(key, unlinks, adds);
      return;
    }

    // Only one side present: release those entries as-is.
    this.releaseAll(key, unlinks, adds);
  }

  private markUncertain(
    identity: FileIdentity,
    reason: 'multiple' | 'hardlink',
    paths: Path[]
  ): void {
    const key = identityKey(identity);
    if (this.uncertain.has(key)) return;
    this.uncertain.add(key);
    this.onUncertain?.(identity, reason, paths);
  }

  /** Clears timers and maps for a matched identity. */
  private clearAll(key: string, unlinks: PendingEntry[], adds: PendingEntry[]): void {
    unlinks.forEach((entry) => this.clearTimer(entry.timer));
    adds.forEach((entry) => this.clearTimer(entry.timer));
    this.pendingUnlinks.delete(key);
    this.pendingAdds.delete(key);
  }

  /** Releases every pending entry for an identity as its original event. */
  private releaseAll(key: string, unlinks: PendingEntry[], adds: PendingEntry[]): void {
    unlinks.forEach((entry) => {
      this.clearTimer(entry.timer);
      this.onUnlink?.(entry.source.path, entry.source);
    });
    adds.forEach((entry) => {
      this.clearTimer(entry.timer);
      this.onAdd?.(entry.source.path, entry.source.stats, entry.source);
    });
    this.pendingUnlinks.delete(key);
    this.pendingAdds.delete(key);
  }

  private expireAddByPath(pathKey: string, entry: PendingEntry): void {
    if (this.addsByPath.get(pathKey) !== entry) return;
    this.addsByPath.delete(pathKey);
    this.onAdd?.(entry.source.path, entry.source.stats, entry.source);
  }

  /**
   * Cancel pending observations at or beneath `path` (e.g. on unwatch).
   * Cancelled entries are released as-is so no event is dropped.
   */
  cancelPath(path: Path): void {
    const pathKey = logicalPathKey(path);
    const prefix = pathKey.endsWith('/') ? pathKey : `${pathKey}/`;
    const within = (candidate: Path): boolean => {
      const key = logicalPathKey(candidate);
      return key === pathKey || key.startsWith(prefix);
    };
    for (const [key, entries] of this.pendingUnlinks) {
      const remaining = entries.filter((entry) => !within(entry.source.path));
      const cancelled = entries.filter((entry) => within(entry.source.path));
      cancelled.forEach((entry) => {
        this.clearTimer(entry.timer);
        this.onUnlink?.(entry.source.path, entry.source);
      });
      if (remaining.length > 0) this.pendingUnlinks.set(key, remaining);
      else this.pendingUnlinks.delete(key);
    }
    for (const [key, entries] of this.pendingAdds) {
      const remaining = entries.filter((entry) => !within(entry.source.path));
      const cancelled = entries.filter((entry) => within(entry.source.path));
      cancelled.forEach((entry) => {
        this.clearTimer(entry.timer);
        this.onAdd?.(entry.source.path, entry.source.stats, entry.source);
      });
      if (remaining.length > 0) this.pendingAdds.set(key, remaining);
      else this.pendingAdds.delete(key);
    }
    for (const [key, entry] of this.addsByPath) {
      if (key === pathKey || key.startsWith(prefix)) {
        this.addsByPath.delete(key);
        this.clearTimer(entry.timer);
        this.onAdd?.(entry.source.path, entry.source.stats, entry.source);
      }
    }
  }

  /** Flush all pending observations as their original events. */
  flush(): void {
    for (const entries of this.pendingUnlinks.values()) {
      entries.forEach((entry) => {
        this.clearTimer(entry.timer);
        this.onUnlink?.(entry.source.path, entry.source);
      });
    }
    for (const entries of this.pendingAdds.values()) {
      entries.forEach((entry) => {
        this.clearTimer(entry.timer);
        this.onAdd?.(entry.source.path, entry.source.stats, entry.source);
      });
    }
    for (const entry of this.addsByPath.values()) {
      this.clearTimer(entry.timer);
      this.onAdd?.(entry.source.path, entry.source.stats, entry.source);
    }
    this.pendingUnlinks.clear();
    this.pendingAdds.clear();
    this.addsByPath.clear();
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.flush();
    this.uncertain.clear();
    this.pathGenerations.clear();
  }
}

/** Sentinel used when an identity cannot be determined. */
const UNKNOWN_IDENTITY: FileIdentity = Object.freeze({ dev: -1, ino: -1 });

export { UNKNOWN_IDENTITY };
