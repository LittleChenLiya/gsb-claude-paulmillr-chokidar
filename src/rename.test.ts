import { describe, it } from '@paulmillr/jsbt/test.js';
import { deepEqual, equal, ok } from 'node:assert/strict';
import type { Stats } from 'node:fs';

import { RenameCorrelator, type CorrelatedEvent } from './rename.js';
import type { Scheduler, SchedulerTimer } from './runtime.js';

/** Deterministic injected clock: time only passes when tests advance it. */
class ManualClock implements Scheduler {
  private time = 0;
  private timers: { at: number; cb: () => void; cleared: boolean }[] = [];
  now(): number {
    return this.time;
  }
  setTimeout(callback: () => void, delay: number): SchedulerTimer {
    const timer = { at: this.time + delay, cb: callback, cleared: false };
    this.timers.push(timer);
    const handle: SchedulerTimer = {
      ref: () => handle,
      unref: () => handle,
    };
    (handle as unknown as { __timer: unknown }).__timer = timer;
    return handle;
  }
  clearTimeout(timer: SchedulerTimer | undefined): void {
    if (!timer) return;
    const entry = (timer as unknown as { __timer?: { cleared: boolean } }).__timer;
    if (entry) entry.cleared = true;
  }
  advance(ms: number): void {
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

const fakeStats = (dev: number, ino: number): Stats => ({ dev, ino }) as Stats;

function setup(windowMs = 50) {
  const clock = new ManualClock();
  const emitted: CorrelatedEvent[] = [];
  const correlator = new RenameCorrelator({
    windowMs,
    scheduler: clock,
    emit: (event) => emitted.push(event),
  });
  return { clock, emitted, correlator };
}

describe('rename correlator', () => {
  it('synthesizes rename from a unique device+ino match', () => {
    const { clock, emitted, correlator } = setup();
    correlator.noteIdentity('old.txt', fakeStats(1, 42));
    const unlinkSource = correlator.unlink('old.txt');
    const addSource = correlator.add('new.txt', fakeStats(1, 42));
    // The tentative pair must survive the wait window first.
    equal(emitted.length, 0);
    clock.advance(50);
    equal(emitted.length, 1);
    const event = emitted[0];
    equal(event.event, 'rename');
    if (event.event !== 'rename') return;
    equal(event.oldPath, 'old.txt');
    equal(event.path, 'new.txt');
    equal(event.uncertain, false);
    deepEqual(
      event.sources.map((source) => [source.event, source.path, source.sequence]),
      [
        ['unlink', 'old.txt', unlinkSource.sequence],
        ['add', 'new.txt', addSource.sequence],
      ]
    );
    clock.advance(1000);
    equal(emitted.length, 1);
  });

  it('correlates an add whose identity arrives via a late stat', () => {
    const { clock, emitted, correlator } = setup();
    correlator.noteIdentity('old.txt', fakeStats(7, 900));
    correlator.unlink('old.txt');
    correlator.add('new.txt');
    const generation = correlator.captureGeneration('new.txt');
    equal(emitted.length, 0);
    equal(correlator.resolveAddStat('new.txt', generation, fakeStats(7, 900)), true);
    clock.advance(50);
    equal(emitted.length, 1);
    equal(emitted[0].event, 'rename');
  });

  it('keeps hard-link multi-candidates independent and uncertain', () => {
    const { clock, emitted, correlator } = setup();
    correlator.noteIdentity('link-a.txt', fakeStats(3, 55));
    correlator.unlink('link-a.txt');
    // Two adds share the same identity (hard links to one inode).
    correlator.add('link-b.txt', fakeStats(3, 55));
    correlator.add('link-c.txt', fakeStats(3, 55));
    equal(emitted.length, 0);
    clock.advance(100);
    equal(emitted.length, 3);
    deepEqual(
      emitted
        .map((event) => [event.event, event.path, event.uncertain])
        .sort((a, b) => String(a[1]).localeCompare(String(b[1]))),
      [
        ['unlink', 'link-a.txt', true],
        ['add', 'link-b.txt', true],
        ['add', 'link-c.txt', true],
      ]
    );
    for (const event of emitted) equal(event.sources.length, 1);
  });

  it('keeps two unlink candidates for one identity independent', () => {
    const { clock, emitted, correlator } = setup();
    correlator.noteIdentity('a.txt', fakeStats(4, 66));
    correlator.noteIdentity('b.txt', fakeStats(4, 66));
    correlator.unlink('a.txt');
    correlator.unlink('b.txt');
    correlator.add('c.txt', fakeStats(4, 66));
    clock.advance(100);
    equal(emitted.length, 3);
    ok(emitted.every((event) => event.event !== 'rename'));
    ok(emitted.every((event) => event.uncertain));
  });

  it('rejects a late stat from a stale path generation', () => {
    const { clock, emitted, correlator } = setup();
    correlator.add('file.txt');
    const staleGeneration = correlator.captureGeneration('file.txt');
    // The path is replaced before the first stat comes back.
    correlator.unlink('file.txt');
    correlator.add('file.txt');
    const currentGeneration = correlator.captureGeneration('file.txt');
    ok(currentGeneration > staleGeneration);
    equal(correlator.resolveAddStat('file.txt', staleGeneration, fakeStats(9, 1)), false);
    equal(correlator.resolveAddStat('file.txt', currentGeneration, fakeStats(9, 2)), true);
    clock.advance(100);
    const events = emitted.map((event) => [event.event, event.path]);
    deepEqual(events, [
      ['add', 'file.txt'],
      ['unlink', 'file.txt'],
      ['add', 'file.txt'],
    ]);
    // The stale stat must not have populated the identity cache either.
    equal(emitted[2].stats?.ino, 2);
  });

  it('never force-merges unknown identities', () => {
    const { clock, emitted, correlator } = setup();
    correlator.unlink('gone.txt');
    correlator.add('appeared.txt');
    clock.advance(100);
    deepEqual(
      emitted.map((event) => [event.event, event.path, event.uncertain]),
      [
        ['unlink', 'gone.txt', false],
        ['add', 'appeared.txt', false],
      ]
    );
  });

  it('holds events for the injected-clock window and never drops them', () => {
    const { clock, emitted, correlator } = setup(50);
    const source = correlator.unlink('lonely.txt');
    clock.advance(49);
    equal(emitted.length, 0);
    clock.advance(1);
    equal(emitted.length, 1);
    equal(emitted[0].event, 'unlink');
    equal(emitted[0].sources[0].sequence, source.sequence);
    equal(emitted[0].sources[0].observedAt, source.observedAt);
  });

  it('flushes an unmatched add once its stat misses the window', () => {
    const { clock, emitted, correlator } = setup(50);
    correlator.add('late.txt');
    clock.advance(50);
    equal(emitted.length, 1);
    equal(emitted[0].event, 'add');
    // A stat resolved after the flush finds no pending entry.
    equal(correlator.resolveAddStat('late.txt', 1, fakeStats(1, 1)), false);
  });

  it('does not match on equal ino with different devices', () => {
    const { clock, emitted, correlator } = setup();
    correlator.noteIdentity('old.txt', fakeStats(1, 42));
    correlator.unlink('old.txt');
    correlator.add('new.txt', fakeStats(2, 42));
    clock.advance(100);
    deepEqual(
      emitted.map((event) => event.event),
      ['unlink', 'add']
    );
  });

  it('runs the five-event example end to end', () => {
    const { clock, emitted, correlator } = setup(50);
    // 1. unlink old.txt (identity known from the live-path cache)
    correlator.noteIdentity('old.txt', fakeStats(1, 42));
    correlator.unlink('old.txt');
    // 2. add new.txt (stat still in flight)
    correlator.add('new.txt');
    // 3. late stat for new.txt arrives with the same unique identity
    const generation = correlator.captureGeneration('new.txt');
    correlator.resolveAddStat('new.txt', generation, fakeStats(1, 42));
    // 4. unlink mystery.txt (identity unknown)
    correlator.unlink('mystery.txt');
    // 5. add other.txt (identity known, no matching unlink)
    correlator.add('other.txt', fakeStats(1, 77));
    equal(emitted.length, 0);
    clock.advance(50);
    deepEqual(
      emitted.map((event) => event.event),
      ['rename', 'unlink', 'add']
    );
    deepEqual(
      emitted.map((event) => event.path),
      ['new.txt', 'mystery.txt', 'other.txt']
    );
    if (emitted[0].event === 'rename') equal(emitted[0].oldPath, 'old.txt');
  });
});

// Self-run when executed directly (node rename.test.js); stays inert on import.
it.runWhen(import.meta.url)?.catch((error: unknown) => {
  console.error(error);
  process.exit(1);
});
