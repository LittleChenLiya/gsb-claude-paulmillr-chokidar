import { describe, it } from '@paulmillr/jsbt/test.js';
import { deepEqual, equal } from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import type { Stats } from 'node:fs';
import {
  RenameCorrelator,
  RENAME_EVENT,
  identityKey,
  statsIdentity,
  UNKNOWN_IDENTITY,
  type FileIdentity,
  type RenameCorrelation,
  type RenameSource,
} from './rename.js';
import type { Scheduler, SchedulerTimer } from './runtime.js';

/**
 * A fake watcher that emits the same `unlink`/`add` observations a real
 * chokidar watcher would, without touching the filesystem. Each event carries
 * a reliable identity (dev:ino) so the correlator can match by identity alone.
 */
class FakeWatcher extends EventEmitter {
  readonly observed: Map<string, FileIdentity> = new Map();

  /** Simulate a file being present in the watcher's tree cache. */
  track(path: string, identity: FileIdentity): void {
    this.observed.set(path, identity);
  }

  /** Simulate an unlink: identity is read from the previous cache. */
  unlink(path: string): void {
    const identity = this.observed.get(path);
    this.observed.delete(path);
    this.emit('unlink', path, identity);
  }

  /** Simulate an add: identity comes from the (possibly late) stat. */
  add(path: string, stats?: Stats): void {
    const identity = stats ? statsIdentity(stats) : undefined;
    if (identity) this.observed.set(path, identity);
    this.emit('add', path, stats, identity);
  }

  /** Simulate a late stat arriving for a previously stat-less add. */
  stat(path: string, stats: Stats): void {
    const identity = statsIdentity(stats);
    if (identity) this.observed.set(path, identity);
    this.emit('stat', path, stats);
  }
}

/** A minimal Stats-like object carrying a reliable identity. */
function fakeStats(dev: number, ino: number): Stats {
  return { dev, ino } as unknown as Stats;
}

/** Virtual scheduler: deterministic, injectable clock. */
class VirtualTimer implements SchedulerTimer {
  active = true;
  callback: () => void;
  due: number;
  constructor(callback: () => void, due: number) {
    this.callback = callback;
    this.due = due;
  }
  ref(): this {
    return this;
  }
  unref(): this {
    return this;
  }
}

class VirtualScheduler implements Scheduler {
  currentTime = 0;
  timers = new Set<VirtualTimer>();

  now(): number {
    return this.currentTime;
  }

  setTimeout(callback: () => void, delay: number): VirtualTimer {
    const timer = new VirtualTimer(callback, this.currentTime + Math.max(0, delay));
    this.timers.add(timer);
    return timer;
  }

  clearTimeout(timer: SchedulerTimer | undefined): void {
    if (!(timer instanceof VirtualTimer)) return;
    timer.active = false;
    this.timers.delete(timer);
  }

  advanceBy(duration: number): void {
    const target = this.currentTime + duration;
    while (true) {
      const next = [...this.timers]
        .filter((timer) => timer.active && timer.due <= target)
        .sort((left, right) => left.due - right.due)[0];
      if (!next) break;
      this.currentTime = next.due;
      next.active = false;
      this.timers.delete(next);
      next.callback();
    }
    this.currentTime = target;
  }

  get activeCount(): number {
    return this.timers.size;
  }
}

type EventRecord =
  | { kind: 'rename'; correlation: RenameCorrelation }
  | { kind: 'unlink'; path: string; source: RenameSource }
  | { kind: 'add'; path: string; stats: Stats | undefined; source: RenameSource }
  | { kind: 'uncertain'; identity: FileIdentity; reason: string; paths: string[] };

function createCorrelator(scheduler: Scheduler, window?: number) {
  const events: EventRecord[] = [];
  const correlator = new RenameCorrelator({
    scheduler,
    window,
    onRename: (correlation) => events.push({ kind: 'rename', correlation }),
    onUnlink: (path, source) => events.push({ kind: 'unlink', path, source }),
    onAdd: (path, stats, source) => events.push({ kind: 'add', path, stats, source }),
    onUncertain: (identity, reason, paths) =>
      events.push({ kind: 'uncertain', identity, reason, paths }),
  });
  return { correlator, events };
}

export function registerRenameTests(): void {
  describe('explicit rename correlator', () => {
    it('should synthesise a rename from unlink/add sharing one unique identity', () => {
      const scheduler = new VirtualScheduler();
      const { correlator, events } = createCorrelator(scheduler);
      const watcher = new FakeWatcher();
      watcher.track('/old.txt', { dev: 1, ino: 100 });
      watcher.on('unlink', (path, identity) => correlator.noteUnlink(path, identity));
      watcher.on('add', (path, stats, identity) => correlator.noteAdd(path, stats, identity));

      watcher.unlink('/old.txt');
      watcher.add('/new.txt', fakeStats(1, 100));
      scheduler.advanceBy(100);

      equal(events.length, 1);
      equal(events[0].kind, 'rename');
      if (events[0].kind !== 'rename') return;
      equal(events[0].correlation.oldPath, '/old.txt');
      equal(events[0].correlation.newPath, '/new.txt');
      deepEqual(events[0].correlation.identity, { dev: 1, ino: 100 });
      equal(events[0].correlation.sources.unlink.path, '/old.txt');
      equal(events[0].correlation.sources.add.path, '/new.txt');
      equal(events[0].correlation.sources.unlink.event, 'unlink');
      equal(events[0].correlation.sources.add.event, 'add');
    });

    it('should keep two candidates independent instead of force-merging', () => {
      const scheduler = new VirtualScheduler();
      const { correlator, events } = createCorrelator(scheduler);
      const watcher = new FakeWatcher();
      watcher.track('/a.txt', { dev: 1, ino: 200 });
      watcher.track('/b.txt', { dev: 1, ino: 200 });
      watcher.on('unlink', (path, identity) => correlator.noteUnlink(path, identity));
      watcher.on('add', (path, stats, identity) => correlator.noteAdd(path, stats, identity));

      watcher.unlink('/a.txt');
      watcher.unlink('/b.txt');
      scheduler.advanceBy(100);

      const unlinks = events.filter((e) => e.kind === 'unlink');
      equal(unlinks.length, 2);
      deepEqual(
        unlinks.map((e) => (e.kind === 'unlink' ? e.path : '')),
        ['/a.txt', '/b.txt']
      );
      const uncertain = events.filter((e) => e.kind === 'uncertain');
      equal(uncertain.length, 1);
      equal(uncertain[0].kind === 'uncertain' && uncertain[0].reason, 'multiple');
    });

    it('should not merge when two adds share one identity', () => {
      const scheduler = new VirtualScheduler();
      const { correlator, events } = createCorrelator(scheduler);
      const watcher = new FakeWatcher();
      watcher.track('/src.txt', { dev: 1, ino: 300 });
      watcher.on('unlink', (path, identity) => correlator.noteUnlink(path, identity));
      watcher.on('add', (path, stats, identity) => correlator.noteAdd(path, stats, identity));

      watcher.unlink('/src.txt');
      watcher.add('/dst-a.txt', fakeStats(1, 300));
      watcher.add('/dst-b.txt', fakeStats(1, 300));
      scheduler.advanceBy(100);

      equal(events.some((e) => e.kind === 'rename'), false);
      const adds = events.filter((e) => e.kind === 'add');
      equal(adds.length, 2);
      equal(events.some((e) => e.kind === 'uncertain'), true);
    });

    it('should not overwrite a new generation with a late old stat', () => {
      const scheduler = new VirtualScheduler();
      const { correlator, events } = createCorrelator(scheduler);
      const watcher = new FakeWatcher();
      watcher.on('add', (path, stats, identity) => correlator.noteAdd(path, stats, identity));
      watcher.on('stat', (path, stats) => correlator.noteStat(path, stats));

      watcher.add('/file.txt');
      watcher.add('/file.txt', fakeStats(1, 400));
      watcher.stat('/file.txt', fakeStats(1, 999));
      scheduler.advanceBy(100);

      const adds = events.filter((e) => e.kind === 'add');
      equal(adds.length, 1);
      equal(adds[0].kind === 'add' && adds[0].source.identity.ino, 400);
    });

    it('should not forcibly merge an event with unknown identity', () => {
      const scheduler = new VirtualScheduler();
      const { correlator, events } = createCorrelator(scheduler);
      const watcher = new FakeWatcher();
      watcher.on('unlink', (path, identity) => correlator.noteUnlink(path, identity));
      watcher.on('add', (path, stats, identity) => correlator.noteAdd(path, stats, identity));

      watcher.unlink('/mystery.txt');
      watcher.add('/new.txt', fakeStats(1, 500));
      scheduler.advanceBy(100);

      equal(events.some((e) => e.kind === 'rename'), false);
      const unlinks = events.filter((e) => e.kind === 'unlink');
      equal(unlinks.length, 1);
      equal(unlinks[0].kind === 'unlink' && unlinks[0].path, '/mystery.txt');
      deepEqual(
        unlinks[0].kind === 'unlink' ? unlinks[0].source.identity : {},
        UNKNOWN_IDENTITY
      );
    });

    it('should release unassociated events after the waiting window', () => {
      const scheduler = new VirtualScheduler();
      const { correlator, events } = createCorrelator(scheduler, 100);
      const watcher = new FakeWatcher();
      watcher.track('/gone.txt', { dev: 1, ino: 600 });
      watcher.on('unlink', (path, identity) => correlator.noteUnlink(path, identity));

      watcher.unlink('/gone.txt');
      equal(events.length, 0);
      scheduler.advanceBy(99);
      equal(events.length, 0);
      scheduler.advanceBy(1);
      equal(events.length, 1);
      equal(events[0].kind, 'unlink');
    });

    it('should keep the original event sources traceable on a rename', () => {
      const scheduler = new VirtualScheduler();
      const { correlator, events } = createCorrelator(scheduler);
      const watcher = new FakeWatcher();
      watcher.track('/a.txt', { dev: 2, ino: 700 });
      watcher.on('unlink', (path, identity) => correlator.noteUnlink(path, identity));
      watcher.on('add', (path, stats, identity) => correlator.noteAdd(path, stats, identity));

      watcher.unlink('/a.txt');
      watcher.add('/b.txt', fakeStats(2, 700));
      scheduler.advanceBy(100);

      equal(events.length, 1);
      const record = events[0];
      if (record.kind !== 'rename') return;
      equal(record.correlation.sources.unlink.event, 'unlink');
      equal(record.correlation.sources.add.event, 'add');
      equal(record.correlation.kind, RENAME_EVENT);
    });

    it('should mark hard links uncertain and never merge them', () => {
      const scheduler = new VirtualScheduler();
      const events: EventRecord[] = [];
      const identity: FileIdentity = { dev: 1, ino: 800 };
      const correlator = new RenameCorrelator({
        scheduler,
        identityExistsElsewhere: (id, exclude) =>
          identityKey(id) === identityKey(identity) && exclude === '/new.txt',
        onRename: (c) => events.push({ kind: 'rename', correlation: c }),
        onUnlink: (path, source) => events.push({ kind: 'unlink', path, source }),
        onAdd: (path, stats, source) => events.push({ kind: 'add', path, stats, source }),
        onUncertain: (id, reason, paths) =>
          events.push({ kind: 'uncertain', identity: id, reason, paths }),
      });
      correlator.noteUnlink('/old.txt', identity);
      correlator.noteAdd('/new.txt', fakeStats(1, 800));
      scheduler.advanceBy(100);

      equal(events.some((e) => e.kind === 'rename'), false);
      equal(events.some((e) => e.kind === 'uncertain' && e.reason === 'hardlink'), true);
    });

    it('should flush pending events on close without dropping them', () => {
      const scheduler = new VirtualScheduler();
      const { correlator, events } = createCorrelator(scheduler);
      correlator.noteUnlink('/a.txt', { dev: 1, ino: 900 });
      correlator.noteAdd('/b.txt', fakeStats(1, 901));
      correlator.close();

      const unlinks = events.filter((e) => e.kind === 'unlink');
      const adds = events.filter((e) => e.kind === 'add');
      equal(unlinks.length, 1);
      equal(adds.length, 1);
    });

    it('should expose identity helpers', () => {
      equal(identityKey({ dev: 1, ino: 2 }), '1:2');
      equal(statsIdentity(fakeStats(3, 4))?.ino, 4);
      equal(statsIdentity(undefined), undefined);
      equal(statsIdentity({ dev: NaN, ino: 1 } as unknown as Stats), undefined);
    });

    it('should correlate a five-event example by identity alone', () => {
      const scheduler = new VirtualScheduler();
      const { correlator, events } = createCorrelator(scheduler);
      const watcher = new FakeWatcher();
      watcher.track('/src/old-name.txt', { dev: 1, ino: 1001 });
      watcher.track('/src/other.txt', { dev: 1, ino: 1002 });
      watcher.on('unlink', (path, identity) => correlator.noteUnlink(path, identity));
      watcher.on('add', (path, stats, identity) => correlator.noteAdd(path, stats, identity));

      // Five events: unlink old, add new (same identity -> rename),
      // unlink other, add two candidates with one identity (independent).
      watcher.unlink('/src/old-name.txt');
      watcher.add('/dst/new-name.txt', fakeStats(1, 1001));
      watcher.unlink('/src/other.txt');
      watcher.add('/dst/other-a.txt', fakeStats(1, 1003));
      watcher.add('/dst/other-b.txt', fakeStats(1, 1003));
      scheduler.advanceBy(100);

      const renames = events.filter((e) => e.kind === 'rename');
      equal(renames.length, 1);
      equal(renames[0].kind === 'rename' && renames[0].correlation.oldPath, '/src/old-name.txt');
      equal(renames[0].kind === 'rename' && renames[0].correlation.newPath, '/dst/new-name.txt');
      const unlinks = events.filter((e) => e.kind === 'unlink');
      equal(unlinks.length, 1);
      equal(unlinks[0].kind === 'unlink' && unlinks[0].path, '/src/other.txt');
      const adds = events.filter((e) => e.kind === 'add');
      equal(adds.length, 2);
      equal(events.filter((e) => e.kind === 'uncertain').length, 1);
    });
  });
}

/** End-to-end FSWatcher integration with the real filesystem. */
import { mkdtemp, rename as fsRename, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import * as sp from 'node:path';
import { FSWatcher } from './index.js';

export function registerRenameIntegrationTests(): void {
  describe('FSWatcher rename option', () => {
    it('should emit a rename event when a file is moved with the same identity', async () => {
      const dir = await mkdtemp(sp.join(tmpdir(), 'chokidar-rename-'));
      const watcher = new FSWatcher({ rename: true, ignoreInitial: true });
      const renames: Array<[string, string]> = [];
      watcher.on('rename', (oldPath, newPath) => renames.push([oldPath, newPath]));
      try {
        const oldPath = sp.join(dir, 'old.txt');
        const newPath = sp.join(dir, 'new.txt');
        await writeFile(oldPath, 'hello');
        await watcher.add(dir);
        await new Promise<void>((resolve) => watcher.once('ready', () => resolve()));
        await fsRename(oldPath, newPath);
        // Wait for the rename window to close.
        await new Promise((resolve) => setTimeout(resolve, 400));
        equal(renames.length, 1);
        equal(renames[0][0], oldPath);
        equal(renames[0][1], newPath);
      } finally {
        await watcher.close();
        await rm(dir, { recursive: true, force: true });
      }
    });

    it('should not emit rename when the rename option is disabled', async () => {
      const dir = await mkdtemp(sp.join(tmpdir(), 'chokidar-rename-off-'));
      const watcher = new FSWatcher({ rename: false, ignoreInitial: true });
      const renames: Array<[string, string]> = [];
      watcher.on('rename', (oldPath, newPath) => renames.push([oldPath, newPath]));
      try {
        const oldPath = sp.join(dir, 'old.txt');
        const newPath = sp.join(dir, 'new.txt');
        await writeFile(oldPath, 'hello');
        await watcher.add(dir);
        await new Promise<void>((resolve) => watcher.once('ready', () => resolve()));
        await fsRename(oldPath, newPath);
        await new Promise((resolve) => setTimeout(resolve, 400));
        equal(renames.length, 0);
      } finally {
        await watcher.close();
        await rm(dir, { recursive: true, force: true });
      }
    });
  });
}
