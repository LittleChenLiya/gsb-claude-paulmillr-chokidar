# Startup adapter

`chokidar/startup` wraps any chokidar watcher (or a compatible event source) and turns the
initial scan into an explicit, one-shot **baseline**, followed by incremental events. Normal
`FSWatcher` behavior is unchanged; the adapter is a strictly opt-in layer on top.

```js
import { watchStartup } from 'chokidar/startup';

const startup = watchStartup('dir', {}, { maxBufferedPaths: 100_000 });
startup.on('baseline', (entries) => {
  // Exactly once: the merged final state of the initial scan.
  // entries: readonly { event: 'add' | 'addDir', path, stats? }[]
});
startup.on('ready', () => console.log('baseline published, now streaming'));
startup.on('change', (path) => console.log('incremental change', path));
startup.on('error', (error) => console.error('startup or watch failure', error));
```

## Startup states

`startup.state` is one of:

| State        | Meaning                                                                               |
| ------------ | ------------------------------------------------------------------------------------- |
| `collecting` | Initial scan in progress. Events are merged into the baseline buffer, none delivered. |
| `streaming`  | The baseline was published exactly once; new events are delivered as increments.      |
| `closed`     | `close()` ran (possibly before `ready`). Terminal.                                    |
| `failed`     | Startup failed (buffer overflow or source error before `ready`). Terminal.            |

`collecting` reaches exactly one terminal or steady outcome: `streaming`, `closed`, or
`failed`. `closed` and `failed` are terminal; `streaming` only exits via `close()`.

## The ready barrier

The source's events and its `ready` signal are placed in one ordered queue. The `ready`
record is a strict barrier:

- Everything queued **before** the barrier is attributed to the baseline.
- Everything queued **after** the barrier is an increment.
- A record is never both: nothing in the baseline is re-delivered as an increment, and no
  increment is folded back into the baseline.

Because attribution depends only on queue order, identical events on the two sides of the
barrier are unambiguous: a `change` queued before `ready` refines the baseline entry, the
same `change` queued after `ready` is delivered as one increment.

## Baseline merge rules

While `collecting`, events for the same path are merged into a single final-state entry:

- `add` / `addDir`: record (or refresh) the entry.
- `change`: update the stats of an existing entry; a `change` for a path with no entry is
  recorded as `add` (the path exists at scan time).
- `unlink` / `unlinkDir`: drop the entry. A path added and removed during the initial scan
  is absent from the baseline.

At `ready` the merged entries are published once as a frozen array on the `baseline` event,
followed by `ready`. Afterwards, `add`, `addDir`, `change`, `unlink`, `unlinkDir` (and
`all`) are forwarded as they arrive.

## Failure rules

- **Buffer limit.** The baseline buffer holds at most `maxBufferedPaths` distinct paths
  (default `1_000_000`). Exceeding the limit fails startup: a
  `StartupBufferOverflowError` (`code: 'CHOKIDAR_STARTUP_BUFFER_OVERFLOW'`) is emitted on
  `error`, the source watcher is closed, the buffer is discarded, and **no baseline is
  published** — a later `ready` from the source is ignored.
- **Source error before `ready`.** Fails startup the same way: the error is forwarded, the
  source is closed, and no (partial, pseudo-complete) baseline is published.
- **Early close.** Calling `close()` before `ready` discards the buffer and closes the
  source. No baseline and no `ready` are ever emitted. `close()` is idempotent.
- **Source error after `ready`.** Forwarded on `error` without closing, matching normal
  watcher behavior.

Events arriving from the source after `closed` / `failed` are dropped. As with any
EventEmitter, an unhandled `error` event throws — always attach an `error` listener.

## API

- `watchStartup(paths, chokidarOptions?, startupOptions?)`: watch paths and return a
  `StartupAdapter`.
- `new StartupAdapter(source, startupOptions?)`: wrap any source with chokidar's event
  names and a `close(): Promise<void>` method (an `FSWatcher` qualifies).
- `startupOptions.maxBufferedPaths`: positive integer, default `1_000_000`.
- `startupOptions.hooks.beforeRecord(record)`: test seam; awaited before each queued
  record is processed, which switches the queue to asynchronous processing and allows
  pausing the queue at an exact position (e.g. at the `ready` barrier).
- `startup.state`, `startup.closed`, `startup.baseline` (the published entries, or
  `undefined`), `startup.whenIdle()` (resolves when the queue has drained),
  `startup.close()`.
