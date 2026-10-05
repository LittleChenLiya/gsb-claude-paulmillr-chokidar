/**
 * Fake watcher driving the explicit rename correlator (src/rename.ts,
 * compiled to rename.js) through a deterministic five-event scenario.
 *
 * Usage: npm run build && node scripts/fake-watcher.mjs
 *
 * The watcher is fake: no real filesystem is touched. Raw observations are
 * scripted, stats arrive "asynchronously" through a manual clock, and the
 * correlator output is printed as JSON lines so the correlation decisions
 * (rename vs. independent events) can be inspected directly.
 */
import { RenameCorrelator } from '../rename.js';

/** Deterministic injected clock; time only passes when advanced by hand. */
class ManualClock {
  constructor() {
    this.time = 0;
    this.timers = [];
  }
  now() {
    return this.time;
  }
  setTimeout(callback, delay) {
    const timer = { at: this.time + delay, cb: callback, cleared: false };
    this.timers.push(timer);
    return {
      timer,
      ref() {
        return this;
      },
      unref() {
        return this;
      },
    };
  }
  clearTimeout(handle) {
    if (handle) handle.timer.cleared = true;
  }
  advance(ms) {
    const target = this.time + ms;
    for (;;) {
      const due = this.timers
        .filter((timer) => !timer.cleared && timer.at <= target)
        .sort((a, b) => a.at - b.at)[0];
      if (!due) break;
      due.cleared = true;
      this.time = due.at;
      due.cb();
    }
    this.time = target;
  }
}

const WINDOW_MS = 50;
const STAT_DELAY_MS = 10;

/** Scripted watcher: emits raw observations, stats lag behind by STAT_DELAY_MS. */
class FakeWatcher {
  constructor(clock, correlator) {
    this.clock = clock;
    this.correlator = correlator;
  }
  /** A live path was statted earlier (initial scan); cache its identity. */
  seed(path, stats) {
    this.correlator.noteIdentity(path, stats);
  }
  unlink(path) {
    console.log(`raw  unlink ${path}`);
    this.correlator.unlink(path);
  }
  add(path, stats) {
    console.log(
      `raw  add    ${path}${stats ? ` dev=${stats.dev} ino=${stats.ino}` : ' (stat pending)'}`
    );
    this.correlator.add(path, stats);
    if (!stats) {
      // The async stat comes back later, keyed to the current path generation.
      const generation = this.correlator.captureGeneration(path);
      this.clock.setTimeout(() => {
        const stats_ = this.pendingStats.get(path);
        if (!stats_) return;
        this.pendingStats.delete(path);
        console.log(`raw  stat   ${path} dev=${stats_.dev} ino=${stats_.ino} gen=${generation}`);
        this.correlator.resolveAddStat(path, generation, stats_);
      }, STAT_DELAY_MS);
    }
  }
  serveStat(path, stats) {
    this.pendingStats.set(path, stats);
  }
  pendingStats = new Map();
}

const clock = new ManualClock();
const correlated = [];
const correlator = new RenameCorrelator({
  windowMs: WINDOW_MS,
  scheduler: clock,
  emit: (event) => {
    correlated.push(event);
    if (event.event === 'rename') {
      console.log(
        `emit rename ${event.oldPath} -> ${event.path}` +
          ` (sources: unlink#${event.sources[0].sequence}, add#${event.sources[1].sequence})`
      );
    } else {
      console.log(
        `emit ${event.event} ${event.path}` +
          `${event.uncertain ? ' [uncertain]' : ''} (source: ${event.event}#${event.sources[0].sequence})`
      );
    }
  },
});

const watcher = new FakeWatcher(clock, correlator);

// Five-event scenario:
watcher.seed('old.txt', { dev: 1, ino: 42 }); // known from the initial scan
watcher.serveStat('new.txt', { dev: 1, ino: 42 });
watcher.unlink('old.txt'); //   1. unlink old.txt (identity from cache)
watcher.add('new.txt'); //      2. add new.txt (stat still in flight)
clock.advance(STAT_DELAY_MS); // 3. late stat: new.txt has the same dev+ino
watcher.unlink('mystery.txt'); //4. unlink mystery.txt (identity unknown)
watcher.add('other.txt', { dev: 1, ino: 77 }); // 5. add other.txt (no match)
clock.advance(WINDOW_MS + 1); // wait window expires; leftovers flush

const kinds = correlated.map((event) => event.event);
console.log(`summary: ${JSON.stringify(kinds)}`);
const expected = ['rename', 'unlink', 'add'];
if (JSON.stringify(kinds) !== JSON.stringify(expected)) {
  console.error(`unexpected correlation result, wanted ${JSON.stringify(expected)}`);
  process.exit(1);
}
console.log('five-event example OK');
