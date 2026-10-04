import { describe, it } from '@paulmillr/jsbt/test.js';
import { deepEqual, equal } from 'node:assert/strict';
import type { Stats } from 'node:fs';
import * as sp from 'node:path';

import { EVENTS as EV, type EmitArgs, type EventName, type FSWInstanceOptions } from './runtime.js';
import { StartupAdapter, type BaselineEntry, type DeliverEvent } from './startup.js';
import { inspectWatcher as internals } from './testing.js';
import type { TestHarness } from './v6.test.js';

type Published = { event: string; args: EmitArgs };

function makeAdapter(overrides: { cwd?: string; limit?: number } = {}) {
  const opts = { cwd: overrides.cwd } as FSWInstanceOptions;
  const adapter = new StartupAdapter(opts, overrides.limit ?? 1024);
  const delivered = [] as Published[];
  const deliver: DeliverEvent = (event, args) => {
    delivered.push({ event, args });
  };
  const publish = (event: EventName, path: string, stats?: Stats) => {
    const args: EmitArgs = [path];
    if (stats !== undefined) args.push(stats);
    adapter.publish(event, args, deliver);
  };
  return { adapter, delivered, publish };
}

function baselinePaths(entries: BaselineEntry[]): string[] {
  return entries.map((entry) => entry.args[0] as string);
}

export function registerStartupTests(context: TestHarness): void {
  const { createSpy, cwatch, dpath, mkdir, waitFor } = context;

  describe('startup adapter', () => {
    it('merges add/change/unlink of one path into the final baseline state', () => {
      const { adapter, delivered, publish } = makeAdapter();
      const a = sp.resolve('/watch/a.txt');
      const b = sp.resolve('/watch/b.txt');

      publish(EV.ADD, a);
      publish(EV.CHANGE, a);
      publish(EV.ADD, b);
      publish(EV.UNLINK, b);

      equal(adapter.buffering, true);
      equal(adapter.size, 1);
      equal(delivered.length, 0);

      const baseline = adapter.seal();
      deepEqual(baselinePaths(baseline), [a]);
      deepEqual(baseline[0].event, EV.ADD);

      adapter.goLive();
      publish(EV.CHANGE, a);
      equal(delivered.length, 1);
      equal(delivered[0].event, EV.CHANGE);
      equal(delivered[0].args[0], a);
    });

    it('keeps a re-added path and refreshes its event kind and stats', () => {
      const { adapter, publish } = makeAdapter();
      const p = sp.resolve('/watch/p');
      const dirStats = { isDirectory: () => true } as unknown as Stats;

      publish(EV.ADD, p);
      publish(EV.UNLINK, p);
      equal(adapter.size, 0);
      publish(EV.ADD_DIR, p, dirStats);

      const baseline = adapter.seal();
      deepEqual(baselinePaths(baseline), [p]);
      equal(baseline[0].event, EV.ADD_DIR);
      equal(baseline[0].args[1], dirStats);
    });

    it('attributes events by barrier side and never double-counts them', () => {
      const a = sp.resolve('/watch/a.txt');

      const before = makeAdapter();
      before.publish(EV.ADD, a);
      before.publish(EV.CHANGE, a);
      const beforeBaseline = before.adapter.seal();
      deepEqual(baselinePaths(beforeBaseline), [a]);
      equal(beforeBaseline[0].event, EV.ADD);
      before.adapter.goLive();
      before.publish(EV.CHANGE, a);
      equal(before.delivered.length, 1);
      equal(before.delivered[0].event, EV.CHANGE);

      const after = makeAdapter();
      after.publish(EV.ADD, a);
      const afterBaseline = after.adapter.seal();
      deepEqual(baselinePaths(afterBaseline), [a]);
      after.adapter.goLive();
      after.publish(EV.CHANGE, a);
      equal(after.delivered.length, 1);
      equal(after.delivered[0].event, EV.CHANGE);

      equal(before.delivered.length + after.delivered.length, 2);
    });

    it('delivers errors immediately while buffering', () => {
      const { adapter, delivered } = makeAdapter();
      const failure = new Error('boom');
      adapter.publish(
        EV.ERROR,
        [failure] as unknown as EmitArgs,
        (event, args) => delivered.push({ event, args })
      );
      equal(delivered.length, 1);
      equal(delivered[0].event, EV.ERROR);
      equal(delivered[0].args[0], failure);
      equal(adapter.buffering, true);
    });

    it('publishes no baseline after an early close', () => {
      const { adapter, delivered, publish } = makeAdapter();
      const a = sp.resolve('/watch/a.txt');
      const b = sp.resolve('/watch/b.txt');
      publish(EV.ADD, a);
      publish(EV.ADD, b);

      adapter.close();
      equal(adapter.state, 'closed');
      deepEqual(adapter.seal(), []);
      publish(EV.CHANGE, a);
      equal(delivered.length, 0);
    });

    it('fails startup and drops the baseline when the buffer overflows', () => {
      const { adapter, publish } = makeAdapter({ limit: 2 });
      const failures: Error[] = [];
      adapter.onFail = (error) => failures.push(error);

      publish(EV.ADD, sp.resolve('/watch/a'));
      publish(EV.ADD, sp.resolve('/watch/b'));
      equal(adapter.state, 'buffering');
      publish(EV.ADD, sp.resolve('/watch/c'));

      equal(adapter.state, 'failed');
      equal(adapter.size, 0);
      equal(failures.length, 1);
      equal(failures[0].message.includes('limit of 2'), true);
      deepEqual(adapter.seal(), []);
    });

    it('keys baseline entries by logical path when cwd is set', () => {
      const cwd = sp.resolve('/project');
      const { adapter, publish } = makeAdapter({ cwd });
      const absolute = sp.join(cwd, 'src/a.txt');
      publish(EV.ADD, absolute);
      publish(EV.CHANGE, absolute);
      equal(adapter.size, 1);
      const baseline = adapter.seal();
      deepEqual(baselinePaths(baseline), [absolute]);
    });
  });

  describe('startup baseline integration', () => {
    it('publishes a merged baseline once at ready then increments', async () => {
      const dir = dpath('baseline-merge');
      await mkdir(dir);
      const watcher = cwatch(dir, { baseline: true, atomic: false });
      const state = internals(watcher);

      let releaseGate!: () => void;
      const gate = new Promise<void>((resolve) => {
        releaseGate = resolve;
      });
      state.trackTask(gate);

      const addSpy = createSpy<EmitArgs, void>();
      const changeSpy = createSpy<EmitArgs, void>();
      const unlinkSpy = createSpy<EmitArgs, void>();
      const readySpy = createSpy<[], void>();
      watcher
        .on(EV.ADD, addSpy)
        .on(EV.CHANGE, changeSpy)
        .on(EV.UNLINK, unlinkSpy)
        .on(EV.READY, readySpy);

      const a = sp.join(dir, 'a.txt');
      const b = sp.join(dir, 'b.txt');

      await state.emitEvent(EV.ADD, a);
      await state.emitEvent(EV.CHANGE, a);
      await state.emitEvent(EV.ADD, b);
      await state.emitEvent(EV.UNLINK, b);

      equal(addSpy.callCount, 0);
      equal(changeSpy.callCount, 0);
      equal(unlinkSpy.callCount, 0);
      equal(readySpy.callCount, 0);

      releaseGate();
      await waitFor([[addSpy, 1, [a]], [readySpy, 1]]);

      equal(addSpy.callCount, 1);
      equal(addSpy.calls[0][0], a);
      equal(changeSpy.callCount, 0);
      equal(unlinkSpy.callCount, 0);

      await state.emitEvent(EV.CHANGE, a);
      await waitFor([[changeSpy, 1, [a]]]);
      equal(changeSpy.callCount, 1);

      await watcher.close();
    });

    it('closes the watcher without a baseline when the buffer overflows', async () => {
      const dir = dpath('baseline-overflow');
      await mkdir(dir);
      const watcher = cwatch(dir, { baseline: true, baselineLimit: 1, atomic: false });
      const state = internals(watcher);

      const errors: unknown[] = [];
      const errorSpy = createSpy<[unknown], void>((error) => errors.push(error));
      watcher.on(EV.ERROR, errorSpy);

      let releaseGate!: () => void;
      const gate = new Promise<void>((resolve) => {
        releaseGate = resolve;
      });
      state.trackTask(gate);

      const a = sp.join(dir, 'a.txt');
      const b = sp.join(dir, 'b.txt');
      await state.emitEvent(EV.ADD, a);
      await state.emitEvent(EV.ADD, b);

      releaseGate();
      await waitFor([[errorSpy, 1]]);
      equal(errors.length, 1);
      equal(watcher.closed, true);
      deepEqual(internals(watcher).readyEmitted, false);

      await watcher.close();
    });

    it('publishes no baseline when closed before ready', async () => {
      const dir = dpath('baseline-early-close');
      await mkdir(dir);
      const watcher = cwatch(dir, { baseline: true, atomic: false });
      const state = internals(watcher);

      const addSpy = createSpy<EmitArgs, void>();
      const readySpy = createSpy<[], void>();
      watcher.on(EV.ADD, addSpy).on(EV.READY, readySpy);

      let releaseGate!: () => void;
      const gate = new Promise<void>((resolve) => {
        releaseGate = resolve;
      });
      state.trackTask(gate);

      await state.emitEvent(EV.ADD, sp.join(dir, 'a.txt'));
      releaseGate();
      await watcher.close();

      equal(addSpy.callCount, 0);
      equal(readySpy.callCount, 0);
    });
  });
}
