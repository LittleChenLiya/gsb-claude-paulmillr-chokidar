#!/usr/bin/env node
/**
 * Explicit rename correlator — fake watcher demo.
 *
 * Runs a five-event example through the correlator using a fake watcher that
 * emits unlink/add observations with reliable identities (dev:ino), without
 * touching the filesystem. The waiting window is driven by an injected clock
 * so the demo is deterministic.
 *
 * Usage: node scripts/rename-demo.mjs
 */

import { RenameCorrelator } from '../rename.js';

/** Minimal Stats-like object carrying a reliable identity. */
function fakeStats(dev, ino) {
  return { dev, ino };
}

/** Injected clock: deterministic, manually advanced. */
class FakeClock {
  currentTime = 0;
  timers = new Map();
  nextId = 0;

  now() {
    return this.currentTime;
  }

  setTimeout(callback, delay) {
    const id = ++this.nextId;
    this.timers.set(id, { callback, due: this.currentTime + delay });
    return id;
  }

  clearTimeout(id) {
    this.timers.delete(id);
  }

  advanceBy(duration) {
    const target = this.currentTime + duration;
    while (true) {
      const next = [...this.timers.entries()]
        .filter(([, timer]) => timer.due <= target)
        .sort(([, a], [, b]) => a.due - b.due)[0];
      if (!next) break;
      const [id, timer] = next;
      this.timers.delete(id);
      this.currentTime = timer.due;
      timer.callback();
    }
    this.currentTime = target;
  }
}

/**
 * A fake watcher that emits the same unlink/add observations a real watcher
 * would, without touching the filesystem. Each event carries a reliable
 * identity so the correlator can match by identity alone.
 */
class FakeWatcher {
  constructor(correlator) {
    this.correlator = correlator;
    /** Simulated tree cache: path -> identity. */
    this.observed = new Map();
  }

  track(path, identity) {
    this.observed.set(path, identity);
  }

  unlink(path) {
    const identity = this.observed.get(path);
    this.observed.delete(path);
    this.correlator.noteUnlink(path, identity);
  }

  add(path, stats) {
    const identity = stats ? { dev: stats.dev, ino: stats.ino } : undefined;
    if (identity) this.observed.set(path, identity);
    this.correlator.noteAdd(path, stats, identity);
  }
}

function main() {
  const clock = new FakeClock();
  const events = [];
  const correlator = new RenameCorrelator({
    scheduler: clock,
    window: 100,
    onRename: (c) => events.push({ kind: 'rename', from: c.oldPath, to: c.newPath }),
    onUnlink: (path) => events.push({ kind: 'unlink', path }),
    onAdd: (path) => events.push({ kind: 'add', path }),
    onUncertain: (identity, reason, paths) =>
      events.push({ kind: 'uncertain', identity: `${identity.dev}:${identity.ino}`, reason, paths }),
  });

  const watcher = new FakeWatcher(correlator);
  watcher.track('/src/old-name.txt', { dev: 1, ino: 1001 });
  watcher.track('/src/other.txt', { dev: 1, ino: 1002 });

  // Five-event example:
  //   1. unlink /src/old-name.txt      (identity 1:1001)
  //   2. add    /dst/new-name.txt     (identity 1:1001) -> rename
  //   3. unlink /src/other.txt        (identity 1:1002)
  //   4. add    /dst/other-a.txt      (identity 1:1003)
  //   5. add    /dst/other-b.txt      (identity 1:1003) -> two candidates, independent
  watcher.unlink('/src/old-name.txt');
  watcher.add('/dst/new-name.txt', fakeStats(1, 1001));
  watcher.unlink('/src/other.txt');
  watcher.add('/dst/other-a.txt', fakeStats(1, 1003));
  watcher.add('/dst/other-b.txt', fakeStats(1, 1003));

  // Close the waiting window so deferred matching runs.
  clock.advanceBy(100);

  correlator.close();

  console.log('Five-event example results:');
  for (const event of events) {
    console.log(' ', JSON.stringify(event));
  }

  const renames = events.filter((e) => e.kind === 'rename');
  const unlinks = events.filter((e) => e.kind === 'unlink');
  const adds = events.filter((e) => e.kind === 'add');
  const uncertain = events.filter((e) => e.kind === 'uncertain');

  const ok =
    renames.length === 1 &&
    renames[0].from === '/src/old-name.txt' &&
    renames[0].to === '/dst/new-name.txt' &&
    unlinks.length === 1 &&
    adds.length === 2 &&
    uncertain.length === 1;

  console.log(ok ? 'PASS' : 'FAIL');
  process.exit(ok ? 0 : 1);
}

main();
