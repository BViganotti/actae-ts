# Actae TypeScript SDK

> Distributed agent coordination and Group Fork are documented in [`../../docs/EXECUTION_GROUPS.md`](../../docs/EXECUTION_GROUPS.md). `client.executionGroup(id)` exposes durable messaging, fenced leases, `waitFor`/`waitAny`/`waitAll`, acknowledgements, promotion, and receipts.

TypeScript client for **Actae** — a real-time event store for agent
workflows. Feature-parity port of the [Python SDK](../python/README.md) and
the [Go SDK](../go/README.md). Node 20+ ESM-only.

```bash
npm install @actae/sdk
```

`@actae/sdk` · Node ≥ 20 · runtime dependency: `ws`.

> **Gotchas at a glance** (full details below): the server never echoes your
> own WebSocket publishes back to you (use `echoSelf`), payload numbers that
> exceed `Number.MAX_SAFE_INTEGER` arrive as `bigint` (use `asInt64`/
> `asBigInt`), and the JSON serializer emits bigint as raw number tokens (so
> values received from `parseJson` round-trip without precision loss).

## Start with self-hosted Actae (free)

Self-hosting is the default way to run Actae: one free, self-contained binary
with the API, dashboard, and PostgreSQL embedded. No account, card, or control
plane.

```bash
# Download for your platform: https://actae.dev/download
tar -xzf actae-<version>-<platform>.tar.gz && chmod +x ./actae
./actae
# The banner ends with your endpoint and a one-time API key:
#   Dashboard:   http://127.0.0.1:8002
#   API key:     sk-...   (shown once — store it now)
```

**Self-hosted still needs an API key** — a *local* key the binary generates and
prints once on first boot, not an account or portal credential. There is no
signup. (Anonymous access is off by default.) Mint more with
`actae keys create <name>`.

```bash
export ACTAE_URL=http://127.0.0.1:8002
export ACTAE_API_KEY=sk-...        # from the banner
```

```ts
import { newClientFromEnv } from '@actae/sdk';
const client = newClientFromEnv(); // reads ACTAE_URL, ACTAE_WS_URL, ACTAE_API_KEY
```

Prefer not to operate it? **Actae Cloud** is the managed runtime — create an
API key in the portal and use your instance URL. The rest of this README is
identical for either deployment.

## 60-second start

For new integrations, prefer the high-level facade:

```ts
import { Actae } from '@actae/sdk';

const actae = Actae.fromEnv();
const charge = actae.effect(
  { key: 'order:{orderId}:charge', name: 'charge' },
  async ({ orderId, amount }) => payments.charge(orderId, amount),
);

await actae.run({ id: 'ticket-42', framework: 'support-agent' }, async () => {
  await charge({ orderId: 'order-7', amount: 2500 });
});
```

Native providers are available as `actae.langgraph`, `actae.langchain`,
`actae.claude`, `actae.openai`, `actae.copilot`, and `actae.codex`. See
[SDK Integration](../../docs/SDK_INTEGRATION.md).

Long-running effects can set `passSignal: true`; Actae appends an
`AbortSignal` so the external client can stop promptly if the caller cancels
or the execution lease is lost.

```ts
import { ActaeClient } from '@actae/sdk';

const client = new ActaeClient({
  apiKey: process.env.ACTAE_API_KEY!,      // from the banner
  endpoint: process.env.ACTAE_URL!,        // http://127.0.0.1:8002
});

// Record an event, then replay it back
const ev = await client.record('my-channel', 'agent.step', { input: 'hello' }, { actor: 'agent' });
const events = await client.replay('my-channel', { limit: 100 });
```

That's the whole loop: **channels** are named event streams, **events** are
immutable JSON blobs with a monotonic cursor, and **replay** reads them back.
Everything else is a convenience or specialization on top of that.
(`newClientFromEnv()` reads `ACTAE_URL`, `ACTAE_WS_URL`, `ACTAE_API_KEY`.)

## API surface

The SDK mirrors the Python SDK's `ActaeClient` 1:1. Every HTTP method is
available directly and through namespaced facades (`client.events.record(...)`
=== `client.record(...)`).

| Area | Client methods | Facade |
|------|---------------|--------|
| Events | `record`, `replay`, `query`, `getCursor`, `latestCursor`, `transition` | `client.events` |
| State | `saveState`, `latestState`, `listStates`, `getState`, `deleteState` | `client.state` |
| Channels/forks | `listChannels`, `fork`, `getForkReceipt`, `resolveStep`, `latestStepNumber`, `getChannelMetadata`, `listForks`, `getForkTree`, `updateMetadata`, `deleteChannel`, `setOutcome`, `promoteChannel`, `diffStates`, `decisionTrail`, `compareChannels` | `client.channels` |
| Experiments | `createExperiment`, `listExperiments`, `addExperimentMember`, `rankExperiment` | — |
| Tool executions | `claimExecution`, `completeExecution`, `failExecution`, `heartbeatExecution`, `cancelExecution`, `getExecution`, `listExecutions`, `deleteExecution` | `client.executions` |
| Consumer groups | `createGroup`, `listGroups`, `deleteGroup`, `joinGroup`, `claimWork`, `ackWork`, `heartbeat`, `groupOffsets` | `client.groups` |
| Wake-ups | `scheduleWakeup`, `listWakeups`, `getWakeup`, `cancelWakeup` | `client.wakeups` |
| Human approval | `requestApproval`, `decideApproval`, `waitForApproval` | — |
| Health/metrics | `healthCheck`, `readinessCheck`, `getMetricsText`, `getMetricsJson` | `client.health` |
| Auth (JWT) | `signup`, `login`, `logout`, `getMe` | `client.auth` |
| WebSocket | `connect`, `disconnect`, `subscribe`, `subscribeAndWait`, `unsubscribe`, `publish`, `stream`, callbacks | `client.ws` |

## Errors

All SDK errors extend `ActaeError`. HTTP/WS failures map to typed classes:

| Situation | Error |
|-----------|-------|
| 401 / bad WS auth | `AuthError` |
| 402 (control-plane lock) | `LockError` |
| transport (refused/timeout/drop) | `ConnectionError` |
| 429 | `RateLimitError` (`retryAfterSeconds`) |
| 404 | `NotFoundError` / `ExecutionNotFoundError` |
| 409 fork boundary | `SnapshotBoundaryError` |
| 409 version guard | `VersionConflictError` |
| 409 idempotency / channel | `IdempotencyConflictError` / `ChannelConflictError` |
| 409 execution claim | `ExecutionNotOwnedError` / `IdempotencyKeyMismatchError` |
| 409 consumer group | `ConsumerError` |
| 409 fork tool policy | `ForkToolBlockedError` |
| session lifecycle | `SessionError` / `SessionCompletedError` / `NoRestorableCheckpointError` |
| wake-up already fired | `WakeupAlreadyFiredError` |
| generic 5xx | `ServerError` |

Use `instanceof` to fork:

```ts
try {
  await client.record(...);
} catch (err) {
  if (err instanceof AuthError) { /* wrong key */ }
  if (err instanceof ConnectionError) { /* retry */ }
}
```

## WebSocket realtime

```ts
await client.connect();
client.onMessage((topic, event) => console.log(topic, event));
await client.subscribe('my-channel', 0);          // cursor 0 = replay from start
const ev = await client.publish('my-channel', { note: 'hi' }); // returns persisted event
await client.unsubscribe('my-channel');
client.disconnect();
```

- `onMessage` callbacks accumulate; `onError`, `onSubscribed`,
  `onDisconnected`, `onReconnect` are single-slot.
- The server **does not echo broadcasts back to the publishing connection** —
  delivery tests use two clients. For a single-client demo set
  `echoSelf: true` (ClientOptions).
- `publish(topic, payload, { operationId })` returns the persisted `Event`
  from the server ack (no HTTP round-trip). A stable `operationId` makes
  retries idempotent.
- `client.stream(topic, cursor?)` returns an async iterator of live events
  (Python `stream()` parity); it ends when the connection drops.
- Auto-reconnect (backoff 0.5s → 30s, cap 10 failures) resubscribes all
  topics; `disconnect()` disables it permanently.

## AgentSession — step / fork / resume

```ts
import { ActaeClient, AgentSession } from '@actae/sdk';

const session = new AgentSession(client, 'my-run', {
  stateFn: () => ({ messages: state.messages }),  // snapshots every step
  snapshotInterval: 1,
});
await session.start();
await session.step('agent.step', { input: { n: 1 }, output: { ok: true } });

// Fork at step 1 → the fork continues at step 2 with steps 1..1's state,
// without re-running step 1.
const fork = await session.fork(1, 'my-run-fix', { reason: 'refine step 2' });
await fork.start();
await fork.step('agent.step', { input: { refined: true }, output: { ok: true } });
```

- `boundaryMode`: `'exact'` (default — raises `NoRestorableCheckpointError`
  when no snapshot exists at the boundary), `'approximate'` (falls back to
  latest state, reports drift via `resolvedBoundaryCursor`),
  `'lineage_only'` (no state copy).
- `AgentSession.resume(client, channelId, { forkAtStep, name })` does crash
  recovery (in-place) or fork-from-existing; raising
  `SessionCompletedError` on completed non-fork resume.
- Boundary provenance is exposed on the session (`forkReceipt`,
  `boundaryRestorable`, `requestedBoundaryCursor`, `resolvedBoundaryCursor`,
  `sourceStateSha256`, `reproducibility`).
- **Intervention + tool policies**: `session.fork(n, name, { intervention,
  toolPolicies })` stores an opaque intervention descriptor (the application
  applies it via `session.intervention`) and a side-effect policy for the
  child (`replay`/`block`/`live`/`auto`, default `auto`) so a fork cannot
  silently re-fire an inherited effect. A blocked call throws
  `ForkToolBlockedError`; `session.toolPolicies` / `forkReceipt.toolPolicies`
  echo the policy.

## StateManager

Framework-agnostic versioned state:

```ts
import { StateManager } from '@actae/sdk';
const mgr = new StateManager(client, 'my-agent');
await mgr.save({ messages: [] });
const state = await mgr.resume({ messages: [] });
await mgr.fork('my-agent-fix', { reason: 'refine' });
```

## Adapters

`@actae/sdk/adapters` exports the shared `ActaeRunContext`,
`CheckpointEnvelope`, `ActaeToolExecutor`, and machine-readable capability
profiles. They provide the complete Actae service surface beside every
framework while accurately distinguishing native checkpoint resume from
context reconstruction and observability-only integrations. See
[`docs/FRAMEWORK_ADAPTER_CONTRACT.md`](../../docs/FRAMEWORK_ADAPTER_CONTRACT.md).

Each adapter lives behind a subpath export and imports its framework lazily —
the core SDK has **no** dependency on LangGraph/LangChain/Claude/OpenAI.

```ts
import { ActaeCheckpointSaver } from '@actae/sdk/adapters/langgraph';
import { ActaeContextSaver, ChainResumer } from '@actae/sdk/adapters/langchain';
import { ActaeClaudeSessionStore } from '@actae/sdk/adapters/claude';
import { installActaeTracing } from '@actae/sdk/adapters/openai';
import { CodexOTLPReceiver } from '@actae/sdk/adapters/codex';
import { CopilotManager } from '@actae/sdk/adapters/copilot';
```

- **LangGraph.js** — `ActaeCheckpointSaver` implements the checkpoint-saver
  protocol (put/putWrites/getTuple/list/deleteThread/get + `forkThread`)
  backed by Actae state snapshots; verified against a real `StateGraph`.
- **LangChain.js** — `ActaeContextSaver` (callback handler saving full
  context every N callbacks) + `ChainResumer` (restart with context);
  verified against a real `CallbackManager`.
- **Claude Agent SDK** — `ActaeClaudeSessionStore` implements the real
  `SessionStore` protocol (camelCase methods, uuid-idempotent appends,
  `foldSessionSummary` summaries, null-for-absent loads, cascade deletes);
  verified against `InMemorySessionStore` + compile-time conformance.
- **OpenAI Agents** — tracing remains available, while
  `actae.openai.run/resume/fork` persist and restore the SDK's native
  serializable `RunState`; verified against the real SDK.
- **Codex CLI** — `CodexOTLPReceiver` hosts an OTLP log endpoint mirroring
  Codex `codex.*` events into per-conversation channels with accumulating
  token/tool snapshots.
- **GitHub Copilot** — `CopilotRecorder` + `CopilotManager` record every
  Copilot session event (dedup, backfill, fork at an event, snapshots) —
  Go `actae/copilot` parity for `@github/copilot-sdk`.

**Known gap**: no CrewAI adapter — CrewAI has no TypeScript port.

## int64 / bigint fidelity

The server (sonic-rs) wraps integers that cannot be represented exactly as
JSON numbers in a marker object. The SDK:

- parses out-of-safe-range integers as `bigint` (never silently rounds);
- unwraps sonic-rs markers recursively;
- exposes `asInt64`, `asBigInt`, `asFloat`, `asString`, `asMap`, `asList`
  accessors mirroring the Go SDK's `As*` helpers;
- serializes outbound `bigint` as raw JSON number tokens (no precision loss,
  no `JSON.stringify` TypeError).

```ts
import { asInt64, asBigInt } from '@actae/sdk';
asInt64(event.payload.count);        // number when safe
asBigInt(event.payload.big_count);   // bigint
```

## Development

```bash
npm install
npm run typecheck     # tsc --noEmit
npm test              # vitest run (336 unit tests; no server needed)
npm run test:coverage # vitest --coverage (~88% lines; src 89%)
npm run build         # compile to dist/
npm run smoke         # live end-to-end vs a running dev Actae
```

Live suite (needs `cd actae && cargo run -- --dev`):

```bash
ACTAE_LIVE=1 npm run test:live
```

Examples (`npm run build` first):

```bash
node examples/quickstart.ts
node examples/session.ts
```

All four framework adapters (LangGraph, LangChain, Claude, OpenAI) are
verified against their **real** SDKs (devDependencies), plus Codex (real HTTP
OTLP export) and Copilot (recorder/manager against a fake session). CI
(`.github/workflows/ci.yml`, `typescript-sdk` job) runs test + typecheck +
build + `npm pack` on every push.
Execution-group members also provide `subscribeWebSocket()`,
`unsubscribeWebSocket()`, and `streamWebSocket()` for cursor-replay plus live
WebSocket delivery of group messages.
