/*! chokidar - MIT License (c) 2012 Paul Miller (paulmillr.com) */
import { describe, it } from '@paulmillr/jsbt/test.js';
import { deepEqual, equal, ok, throws } from 'node:assert/strict';
import { EventEmitter, once } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { appendFile, writeFile } from 'node:fs/promises';
import * as sp from 'node:path';
import {
  type BaselineEntry,
  STARTUP_BUFFER_OVERFLOW,
  StartupAdapter,
  StartupBufferOverflowError,
  type StartupIncrementEvent,
  watchStartup,
} from './startup.js';

const TIMEOUT = 10000;

/** Controllable fake watcher: the test scripts every emission. */
class FakeWatcher extends EventEmitter {
  closeCalls: number = 0;
  async close(): Promise<void> {
    this.closeCalls += 1;
  }
}

type LogEntry =
  | { type: 'baseline'; entries: readonly BaselineEntry[] }
  | { type: 'ready' }
  | { type: 'error'; error: Error }
  | { type: 'increment'; event: StartupIncrementEvent; path: string };

function record(adapter: StartupAdapter): LogEntry[] {
  const log: LogEntry[] = [];
  adapter.on('baseline', (entries) => log.push({ type: 'baseline', entries }));
  adapter.on('ready', () => log.push({ type: 'ready' }));
  adapter.on('error', (error) => log.push({ type: 'error', error }));
  for (const event of ['add', 'addDir', 'change', 'unlink', 'unlinkDir'] as const) {
    adapter.on(event, (path) => log.push({ type: 'increment', event, path }));
  }
  return log;
}

function baselinePaths(log: LogEntry[]): string[] {
  const baselines = log.filter((entry) => entry.type === 'baseline');
  equal(baselines.length, 1, 'exactly one baseline must be published');
  return baselines[0].entries.map((entry) => entry.path);
}

function increments(log: LogEntry[]): Array<[string, string]> {
  return log
    .filter(
      (entry): entry is Extract<LogEntry, { type: 'increment' }> => entry.type === 'increment'
    )
    .map((entry) => [entry.event, entry.path]);
}

function errors(log: LogEntry[]): Error[] {
  return log
    .filter((entry): entry is Extract<LogEntry, { type: 'error' }> => entry.type === 'error')
    .map((entry) => entry.error);
}

function withTimeout<T>(promise: Promise<T>, label: string): Promise<T> {
  return Promise.race([
    promise,
    new Promise<T>((_resolve, reject) =>
      setTimeout(() => reject(new Error(`timeout waiting for ${label}`)), TIMEOUT)
    ),
  ]);
}

describe('startup adapter', () => {
  it('merges the initial scan into a baseline and streams increments after ready', async () => {
    const source = new FakeWatcher();
    const adapter = new StartupAdapter(source);
    const log = record(adapter);
    equal(adapter.state, 'collecting');

    source.emit('add', 'a');
    source.emit('change', 'a');
    source.emit('add', 'b');
    source.emit('unlink', 'b');
    source.emit('ready');
    source.emit('change', 'a');
    await adapter.whenIdle();

    // add+change merged into one baseline entry; add+unlink vanished.
    deepEqual(baselinePaths(log), ['a']);
    const baseline = adapter.baseline!;
    deepEqual(
      baseline.map((entry) => [entry.event, entry.path]),
      [['add', 'a']]
    );
    // Baseline is published before ready, increments come after.
    deepEqual(
      log.map((entry) => entry.type),
      ['baseline', 'ready', 'increment']
    );
    // The post-ready change is exactly one increment, not a baseline duplicate.
    deepEqual(increments(log), [['change', 'a']]);
    equal(adapter.state, 'streaming');
    equal(errors(log).length, 0);
    await adapter.close();
    equal(adapter.state, 'closed');
    equal(source.closeCalls, 1);
  });

  it('attributes identical events around the ready barrier by queue order', async () => {
    const source = new FakeWatcher();
    // Controllable queue: hold processing exactly at the ready barrier.
    let releaseBarrier!: () => void;
    const barrierHeld = new Promise<void>((resolve) => {
      let released = false;
      releaseBarrier = () => {
        if (!released) {
          released = true;
          resolve();
        }
      };
    });
    let atBarrier!: () => void;
    const reachedBarrier = new Promise<void>((resolve) => {
      atBarrier = resolve;
    });
    const adapter = new StartupAdapter(source, {
      hooks: {
        beforeRecord: async (record) => {
          if (record.kind === 'ready') {
            atBarrier();
            await barrierHeld;
          }
        },
      },
    });
    const log = record(adapter);

    source.emit('add', 'a');
    source.emit('change', 'a'); // same event, before the barrier
    source.emit('ready');
    await reachedBarrier; // queue is parked at the barrier; pre-ready records are merged
    source.emit('change', 'a'); // same event, emitted while the queue is held at the barrier
    releaseBarrier();
    await adapter.whenIdle();

    // The pre-barrier change only refined the baseline; the post-barrier one
    // is a single increment. Nothing is both baseline and increment.
    deepEqual(baselinePaths(log), ['a']);
    deepEqual(increments(log), [['change', 'a']]);
    equal(adapter.state, 'streaming');
    await adapter.close();
  });

  it('publishes no baseline on early close and ignores late source events', async () => {
    const source = new FakeWatcher();
    const adapter = new StartupAdapter(source);
    const log = record(adapter);

    source.emit('add', 'a');
    await adapter.close();
    equal(adapter.state, 'closed');
    equal(adapter.baseline, undefined);
    // Late emissions from a not-yet-stopped source are dropped.
    source.emit('ready');
    source.emit('add', 'b');
    await adapter.whenIdle();

    deepEqual(log, []);
    equal(source.closeCalls, 1);
    // close() is idempotent.
    await adapter.close();
    equal(source.closeCalls, 1);
  });

  it('fails startup and closes the watcher when the buffer limit is exceeded', async () => {
    const source = new FakeWatcher();
    const adapter = new StartupAdapter(source, { maxBufferedPaths: 2 });
    const log = record(adapter);

    source.emit('add', 'a');
    source.emit('addDir', 'dir');
    source.emit('add', 'c'); // third distinct path: overflow
    source.emit('ready'); // must not publish a pseudo-complete baseline
    source.emit('add', 'd');
    await adapter.whenIdle();
    await adapter.close();

    equal(adapter.state, 'failed');
    equal(adapter.closed, true);
    const errs = errors(log);
    equal(errs.length, 1);
    ok(errs[0] instanceof StartupBufferOverflowError);
    equal((errs[0] as StartupBufferOverflowError & { code: string }).code, STARTUP_BUFFER_OVERFLOW);
    deepEqual(
      log.map((entry) => entry.type),
      ['error']
    );
    equal(adapter.baseline, undefined);
    equal(source.closeCalls, 1);
  });

  it('validates the buffer limit', () => {
    throws(() => new StartupAdapter(new FakeWatcher(), { maxBufferedPaths: 0 }), TypeError);
    throws(() => new StartupAdapter(new FakeWatcher(), { maxBufferedPaths: 1.5 }), TypeError);
  });

  it('fails startup without a baseline when the source errors before ready', async () => {
    const source = new FakeWatcher();
    const adapter = new StartupAdapter(source);
    const log = record(adapter);

    source.emit('add', 'a');
    source.emit('error', new Error('boom'));
    source.emit('ready');
    source.emit('add', 'b');
    await adapter.whenIdle();
    await adapter.close();

    equal(adapter.state, 'failed');
    deepEqual(
      errors(log).map((error) => error.message),
      ['boom']
    );
    deepEqual(
      log.map((entry) => entry.type),
      ['error']
    );
    equal(adapter.baseline, undefined);
    equal(source.closeCalls, 1);
  });

  it('forwards source errors after ready without closing', async () => {
    const source = new FakeWatcher();
    const adapter = new StartupAdapter(source);
    const log = record(adapter);

    source.emit('add', 'a');
    source.emit('ready');
    source.emit('error', new Error('late boom'));
    source.emit('change', 'a');
    await adapter.whenIdle();

    equal(adapter.state, 'streaming');
    deepEqual(
      errors(log).map((error) => error.message),
      ['late boom']
    );
    deepEqual(increments(log), [['change', 'a']]);
    equal(source.closeCalls, 0);
    await adapter.close();
  });
});

describe('startup adapter over a real watcher (node)', () => {
  it('publishes the scan as baseline and streams later changes', async () => {
    const dir = mkdtempSync(sp.join(process.cwd(), '.tmp-startup-'));
    // The file exists before watching, so it deterministically lands in the baseline.
    await writeFile(sp.join(dir, 'a.txt'), 'hello');
    // Polling with a short interval keeps the increments deterministic.
    const adapter = watchStartup(dir, { backend: 'polling', pollingInterval: 50 });
    adapter.on('error', () => {}); // errors are asserted via state, not crashes
    try {
      const [entries] = (await withTimeout(once(adapter, 'baseline'), 'baseline')) as [
        readonly BaselineEntry[],
      ];
      // The watched root itself is reported as addDir, the file as add.
      deepEqual(entries.map((entry) => [entry.event, sp.basename(entry.path)]).sort(), [
        ['add', 'a.txt'],
        ['addDir', sp.basename(dir)],
      ]);
      equal(adapter.state, 'streaming');

      // Let a few poll cycles pass so the append lands in a later cycle.
      await new Promise((resolve) => setTimeout(resolve, 200));
      const changed = withTimeout(
        new Promise<string>((resolve) => adapter.on('change', (path) => resolve(path))),
        'change increment'
      );
      await appendFile(sp.join(dir, 'a.txt'), '!');
      equal(sp.basename(await changed), 'a.txt');

      const added = withTimeout(
        new Promise<string>((resolve) => adapter.on('add', (path) => resolve(path))),
        'add increment'
      );
      await writeFile(sp.join(dir, 'b.txt'), 'x');
      equal(sp.basename(await added), 'b.txt');
    } finally {
      await adapter.close();
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

await it.run(true);
