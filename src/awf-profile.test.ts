/**
 * Node-targeted acceptance tests for the awaitWriteFinish stability profile.
 *
 * Run with Node.js only (after `npm run build`):
 *   node --test awf-profile.test.js
 * or via npm:
 *   npm run test:awf
 *
 * Everything is deterministic: the clock and `stat` are injected, no real
 * files are touched.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import type { Stats } from 'node:fs';
import * as sp from 'node:path';
import { FSWatcher } from './index.js';
import { EVENTS as EV, type Scheduler, type SchedulerTimer, type StatFile } from './runtime.js';
import { inspectWatcher as internals } from './testing.js';

class VirtualTimer implements SchedulerTimer {
  active = true;
  readonly scheduler: VirtualScheduler;
  readonly callback: () => void;
  readonly due: number;
  readonly order: number;
  constructor(scheduler: VirtualScheduler, callback: () => void, due: number, order: number) {
    this.scheduler = scheduler;
    this.callback = callback;
    this.due = due;
    this.order = order;
  }
  ref(): this {
    return this;
  }
  unref(): this {
    return this;
  }
  cancel(): void {
    this.active = false;
    this.scheduler.timers.delete(this);
  }
}

class VirtualScheduler implements Scheduler {
  currentTime = 0;
  private nextOrder = 0;
  readonly timers = new Set<VirtualTimer>();

  now(): number {
    return this.currentTime;
  }
  setTimeout(callback: () => void, delay: number): VirtualTimer {
    const timer = new VirtualTimer(
      this,
      callback,
      this.currentTime + Math.max(0, delay),
      this.nextOrder++
    );
    this.timers.add(timer);
    return timer;
  }
  clearTimeout(timer: SchedulerTimer | undefined): void {
    if (timer instanceof VirtualTimer && timer.active) timer.cancel();
  }
  advanceBy(duration: number): void {
    const target = this.currentTime + duration;
    while (true) {
      const next = [...this.timers]
        .filter((timer) => timer.active && timer.due <= target)
        .sort((left, right) => left.due - right.due || left.order - right.order)[0];
      if (!next) break;
      this.currentTime = next.due;
      next.active = false;
      this.timers.delete(next);
      next.callback();
    }
    this.currentTime = target;
  }
}

/** Minimal Stats stand-in; the policy only reads size, mtimeMs, dev and ino. */
function fakeStats(size: number, mtimeMs: number, ino: number, dev = 1): Stats {
  return {
    size,
    mtimeMs,
    ino,
    dev,
    isFile: () => true,
    isDirectory: () => false,
    isSymbolicLink: () => false,
  } as unknown as Stats;
}

type EmittedEvent = [event: string, path: string, stats?: Stats];

function createWatcher(
  statFile: StatFile,
  awf: { pollInterval: number; stabilityThreshold: number }
) {
  const scheduler = new VirtualScheduler();
  const watcher = new FSWatcher({ atomic: false, awaitWriteFinish: awf }, scheduler, statFile);
  internals(watcher).readyEmitted = true;
  const events: EmittedEvent[] = [];
  watcher.on(EV.ALL, (event: string, path: string, stats?: Stats) => {
    events.push([event, path, stats]);
  });
  const advance = async (ms: number): Promise<void> => {
    scheduler.advanceBy(ms);
    await internals(watcher).drainTasks();
  };
  return { scheduler, watcher, events, advance };
}

const AWF = { pollInterval: 50, stabilityThreshold: 100 };

describe('awaitWriteFinish stability profile', () => {
  it('four snapshots: same-size mtime changes reset the quiet window, then fire once', async () => {
    // Four injected stat snapshots of one file at one path. Size never
    // changes; mtime moves between S1->S2->S3 and settles at S4.
    const snapshots = [
      fakeStats(10, 100, 7), // S1: first sample
      fakeStats(10, 200, 7), // S2: same size, mtime moved -> window resets
      fakeStats(10, 300, 7), // S3: same size, mtime moved -> window resets
      fakeStats(10, 300, 7), // S4: stable -> the quiet window runs to completion
    ];
    let statCalls = 0;
    const statFile: StatFile = async () => snapshots[Math.min(statCalls++, snapshots.length - 1)];
    const { watcher, events, advance } = createWatcher(statFile, AWF);
    const filePath = sp.resolve('four-snapshots.txt');

    await internals(watcher).emitEvent(EV.ADD, filePath);

    await advance(50); // t=50:  S1 sampled, nothing to compare yet
    await advance(50); // t=100: S2, mtime moved -> window restarts here
    await advance(50); // t=150: S3, mtime moved -> window restarts here
    await advance(50); // t=200: S4 stable, only 50ms quiet so far
    assert.equal(statCalls, 4);
    assert.equal(events.length, 0, 'must not fire early on same-size mtime changes');

    await advance(50); // t=250: S4 again, quiet window (150..250) is full
    assert.equal(events.length, 1, 'fires once the final quiet window is full');
    assert.deepEqual(events[0].slice(0, 2), [EV.ADD, filePath]);
    assert.equal(events[0][2]!.mtimeMs, 300);

    await advance(200); // nothing re-fires afterwards
    assert.equal(events.length, 1);
    assert.equal(internals(watcher).pendingWrites.size, 0);

    const report = internals(watcher).stabilityReports.get(internals(watcher).logicalKey(filePath));
    assert.equal(report!.identityKnown, true);
    assert.equal(report!.generations, 0);
    await watcher.close();
  });

  it('discards old-generation stats after the path is replaced', async () => {
    // Stat promises are held by the test and resolved out of band.
    const resolvers: Array<(stats: Stats) => void> = [];
    const statFile: StatFile = () => new Promise<Stats>((resolve) => resolvers.push(resolve));
    const scheduler = new VirtualScheduler();
    const watcher = new FSWatcher({ atomic: false, awaitWriteFinish: AWF }, scheduler, statFile);
    internals(watcher).readyEmitted = true;
    const events: EmittedEvent[] = [];
    watcher.on(EV.ALL, (event: string, path: string, stats?: Stats) => {
      events.push([event, path, stats]);
    });
    const filePath = sp.resolve('replaced.txt');
    const key = internals(watcher).logicalKey(filePath);

    await internals(watcher).emitEvent(EV.ADD, filePath);
    scheduler.advanceBy(50); // t=50: inspect starts, stat of the old file is in flight
    assert.equal(resolvers.length, 1);

    // The path is replaced by a new file: a fresh ADD opens a new generation.
    await internals(watcher).emitEvent(EV.ADD, filePath);
    assert.equal(internals(watcher).pendingWrites.get(key)?.generation, 1);

    // The old file's stat arrives late and must be discarded, not published.
    resolvers[0](fakeStats(10, 100, 1));
    await internals(watcher).drainTasks();
    assert.equal(events.length, 0);
    assert.equal(internals(watcher).pendingWrites.size, 1);

    // The next polls observe the new file; its quiet window started at t=50.
    scheduler.advanceBy(50); // t=100
    assert.equal(resolvers.length, 2);
    resolvers[1](fakeStats(20, 500, 2));
    await internals(watcher).drainTasks();
    assert.equal(events.length, 0);

    scheduler.advanceBy(50); // t=150: window (50..150) is full
    assert.equal(resolvers.length, 3);
    resolvers[2](fakeStats(20, 500, 2));
    await internals(watcher).drainTasks();

    assert.equal(events.length, 1);
    assert.deepEqual(events[0].slice(0, 2), [EV.ADD, filePath]);
    assert.equal(events[0][2]!.ino, 2, 'publishes the new file, never the old stat');

    const report = internals(watcher).stabilityReports.get(key);
    assert.equal(report!.generations, 1);
    await watcher.close();
  });

  it('marks unknown identity explicitly instead of assuming the same file', async () => {
    // ino of 0 means the filesystem reports no reliable identity.
    const statFile: StatFile = async () => fakeStats(5, 100, 0);
    const { watcher, events, advance } = createWatcher(statFile, AWF);
    const filePath = sp.resolve('unknown-identity.txt');

    await internals(watcher).emitEvent(EV.ADD, filePath);
    await advance(50); // t=50: first sample
    await advance(50); // t=100: quiet window full (degraded size+mtime policy)

    assert.equal(events.length, 1, 'still fires under the degraded guarantee');
    const report = internals(watcher).stabilityReports.get(internals(watcher).logicalKey(filePath));
    assert.ok(report, 'stability report is recorded');
    assert.equal(report!.identityKnown, false, 'unknown identity is explicitly marked');
    assert.equal(report!.profile.identity.kind, 'unknown');
    await watcher.close();
  });

  it('opens a new generation when stat identity reveals a replacement', async () => {
    // Same size and mtime throughout; only the reliable identity changes.
    const snapshots = [
      fakeStats(10, 100, 7), // old file
      fakeStats(10, 100, 7),
      fakeStats(10, 100, 8), // replaced by a new file at the same path
      fakeStats(10, 100, 8),
      fakeStats(10, 100, 8),
      fakeStats(10, 100, 8),
    ];
    let statCalls = 0;
    const statFile: StatFile = async () => snapshots[Math.min(statCalls++, snapshots.length - 1)];
    const { watcher, events, advance } = createWatcher(statFile, {
      pollInterval: 50,
      stabilityThreshold: 150,
    });
    const filePath = sp.resolve('identity-replacement.txt');

    await internals(watcher).emitEvent(EV.ADD, filePath);
    await advance(50); // t=50:  old file, first sample
    await advance(50); // t=100: old file, stable
    await advance(50); // t=150: identity moved 7 -> 8: new generation, window restarts
    assert.equal(events.length, 0);
    await advance(100); // t=250: only 100ms quiet for the new file
    assert.equal(events.length, 0, 'replacement restarts the quiet window');
    await advance(50); // t=300: window (150..300) is full
    assert.equal(events.length, 1);
    assert.equal(events[0][2]!.ino, 8);

    const report = internals(watcher).stabilityReports.get(internals(watcher).logicalKey(filePath));
    assert.equal(report!.generations, 1);
    assert.equal(report!.identityKnown, true);
    await watcher.close();
  });

  it('cancels the current wait on unlink', async () => {
    let statCalls = 0;
    const statFile: StatFile = async () => {
      statCalls++;
      return fakeStats(5, 100, 7);
    };
    const { watcher, events, advance } = createWatcher(statFile, AWF);
    const filePath = sp.resolve('unlinked-during-wait.txt');

    await internals(watcher).emitEvent(EV.ADD, filePath);
    assert.equal(internals(watcher).pendingWrites.size, 1);

    await internals(watcher).emitEvent(EV.UNLINK, filePath);
    assert.equal(internals(watcher).pendingWrites.size, 0, 'unlink cancels the wait');
    assert.deepEqual(
      events.map(([event, path]) => [event, path]),
      [[EV.UNLINK, filePath]]
    );

    await advance(500);
    assert.equal(statCalls, 0, 'no poll survives the cancellation');
    assert.equal(events.length, 1, 'no late add is published');
    await watcher.close();
  });

  it('cancels the current wait on close', async () => {
    const statFile: StatFile = async () => fakeStats(5, 100, 7);
    const { watcher, events, advance } = createWatcher(statFile, AWF);
    const filePath = sp.resolve('closed-during-wait.txt');

    await internals(watcher).emitEvent(EV.ADD, filePath);
    assert.equal(internals(watcher).pendingWrites.size, 1);

    await watcher.close();
    assert.equal(internals(watcher).pendingWrites.size, 0, 'close cancels the wait');

    await advance(500);
    assert.equal(events.length, 0, 'nothing is published after close');
  });
});
