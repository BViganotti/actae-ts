/**
 * Deep fork-resume comparison & edge-case suite (live) — TS port of the
 * Python `sdks/python/tests/test_fork_resume_deep.py`.
 *
 * The core question this answers:
 *   - A FULL run (steps 1-9, fresh state) vs a FORK run (fork at step 5, run
 *     steps 6-9): are the results identical for the shared steps?
 *   - A FULL run vs a FORK with a MODIFIED prompt at step 6: does the result
 *     differ ONLY in the refined step, while steps 1-5 stay byte-identical?
 *   - Every edge case: fork at step 1 / last / beyond range, fork-of-fork,
 *     fork with no saved state, concurrent forks, idempotent forks,
 *     crash recovery.
 *
 * Uses a deterministic pipeline (no LLM) so assertions are exact.
 *
 *     ACTAE_LIVE=1 npm run test:live -- test/fork_deep_live.test.ts
 */

import { describe, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import {
  ActaeClient,
  AgentSession,
  NoRestorableCheckpointError,
  SessionCompletedError,
  SessionError,
  type Event,
  type JsonObject,
} from '../src/index.js';

const LIVE = process.env.ACTAE_LIVE === '1';
const url = process.env.ACTAE_URL ?? 'http://localhost:8002';
const apiKey = process.env.ACTAE_API_KEY ?? 'sk-dev-0000000000000000000000';

function client(): ActaeClient {
  return new ActaeClient({ apiKey, endpoint: url, timeout: 15000 });
}

function chan(prefix: string): string {
  return `ts-fork-deep-${prefix}-${randomUUID()}`;
}

async function stateOf(c: ActaeClient, channel: string): Promise<JsonObject> {
  const snap = await c.latestState(channel);
  return snap?.state ?? {};
}

async function typesOf(c: ActaeClient, channel: string): Promise<string[]> {
  const events = await c.replay(channel, { limit: 500 });
  return events.map((e) => e.eventType);
}

// Deterministic 9-step pipeline. Each step produces a value that depends on
// all previous steps' values (so any deviation is caught). Step 6 is the
// "refined" step — its output depends on a `style` parameter.
const ALL_STATE_KEYS = [
  's1_out', 's2_out', 's3_out', 's4_out', 's5_out',
  's6_out', 's7_out', 's8_out', 's9_out',
];
const FORK_STEP = 5;

class Registry {
  private calls: Record<string, number> = {};

  mark(step: string): void {
    this.calls[step] = (this.calls[step] ?? 0) + 1;
  }

  count(step: string): number {
    return this.calls[step] ?? 0;
  }
}

function prevOut(state: JsonObject, n: number): string {
  const parts: string[] = [];
  for (let i = 1; i < n; i++) parts.push(state[`s${i}_out`] as string);
  return parts.join('|');
}

type StepFn = (reg: Registry, state: JsonObject, style?: string) => JsonObject;

const step1: StepFn = (reg, state) => {
  reg.mark('s1');
  return { s1_out: `r:${state['topic'] as string}` };
};
const step2: StepFn = (reg, state) => {
  reg.mark('s2');
  return { s2_out: `i(${prevOut(state, 2)})` };
};
const step3: StepFn = (reg, state) => {
  reg.mark('s3');
  return { s3_out: `sh(${prevOut(state, 3)})` };
};
const step4: StepFn = (reg, state) => {
  reg.mark('s4');
  return { s4_out: `rk(${prevOut(state, 4)})` };
};
const step5: StepFn = (reg, state) => {
  reg.mark('s5');
  return { s5_out: `m(${prevOut(state, 5)})` };
};
const step6: StepFn = (reg, state, style) => {
  reg.mark('s6');
  return { s6_out: `[${style ?? 'neutral'}] rec(${prevOut(state, 6)})` };
};
const step7: StepFn = (reg, state) => {
  reg.mark('s7');
  return { s7_out: `sum(${prevOut(state, 7)})` };
};
const step8: StepFn = (reg, state) => {
  reg.mark('s8');
  return { s8_out: `nxt(${prevOut(state, 8)})` };
};
const step9: StepFn = (reg, state) => {
  reg.mark('s9');
  return { s9_out: `fin(${prevOut(state, 9)})` };
};

const PIPELINE: Array<{ step: number; fn: StepFn }> = [
  { step: 1, fn: step1 },
  { step: 2, fn: step2 },
  { step: 3, fn: step3 },
  { step: 4, fn: step4 },
  { step: 5, fn: step5 },
  { step: 6, fn: step6 },
  { step: 7, fn: step7 },
  { step: 8, fn: step8 },
  { step: 9, fn: step9 },
];

async function runPipeline(
  session: AgentSession,
  reg: Registry,
  live: JsonObject,
  opts: { style?: string; fromStep?: number; stopAfter?: number } = {},
): Promise<void> {
  const style = opts.style ?? 'neutral';
  const fromStep = opts.fromStep ?? 1;
  const stopAfter = opts.stopAfter ?? 9;
  for (const { step, fn } of PIPELINE) {
    if (step < fromStep) continue;
    if (step > stopAfter) break;
    const delta = fn(reg, live, style);
    Object.assign(live, delta);
    let stepInput: JsonObject = { ...delta };
    if (step === 6) {
      // Step 6's input is the full inherited context (all live keys present).
      stepInput = {};
      for (const k of [...ALL_STATE_KEYS, 's6_out']) {
        if (k in live) {
          const v = live[k];
          if (v !== undefined) stepInput[k] = v;
        }
      }
    }
    await session.step(`step.${step}`, { input: stepInput, output: delta, context: delta });
  }
}

describe.runIf(LIVE)('live deep fork-resume (port of Python test_fork_resume_deep)', () => {
  // 1. FULL run vs FORK run (same prompt): identical results for shared steps.
  it('full run vs fork: byte-identical state for all 9 steps; steps 1-5 not re-invoked', async () => {
    const c = client();
    const base = chan('cmp');
    const fork = `${base}-fork`;

    // FULL: run all 9 steps from scratch.
    const fullReg = new Registry();
    const fullLive: JsonObject = { topic: 'T' };
    const fullSession = new AgentSession(c, base, { stateFn: () => ({ ...fullLive }) });
    await fullSession.start();
    await runPipeline(fullSession, fullReg, fullLive);
    await fullSession.complete();
    const fullState = await stateOf(c, base);

    // FORK at step 5, run steps 6-9 with the SAME step-6 prompt.
    const forkReg = new Registry();
    const forkLive: JsonObject = { topic: 'T' };
    const forkSess = await AgentSession.resume(c, base, {
      forkAtStep: FORK_STEP,
      name: fork,
      stateFn: () => ({ ...forkLive }),
    });
    Object.assign(forkLive, forkSess.inheritedState ?? {});
    await forkSess.start();
    await runPipeline(forkSess, forkReg, forkLive, { fromStep: FORK_STEP + 1 });
    await forkSess.complete();
    const forkState = await stateOf(c, fork);

    // (a) Steps 1-5 identical (inherited, not regenerated).
    for (let i = 1; i <= 5; i++) {
      expect(forkState[`s${i}_out`]).toBe(fullState[`s${i}_out`]);
    }
    // (b) Steps 6-9 identical too (same prompt, same inputs → same outputs).
    for (let i = 6; i <= 9; i++) {
      expect(forkState[`s${i}_out`]).toBe(fullState[`s${i}_out`]);
    }
    // (c) Full determinism: the whole state matches.
    expect(forkState).toEqual(fullState);
    // (d) Event logs: fork has fork.started + session.started + steps 6-9.
    const forkTypes = await typesOf(c, fork);
    expect(forkTypes[0]).toBe('fork.started');
    expect(forkTypes).toContain('step.6');
    expect(forkTypes).toContain('step.9');
    for (const t of ['step.1', 'step.2', 'step.3', 'step.4', 'step.5']) {
      expect(forkTypes).not.toContain(t);
    }
    // (e) Execution counters: steps 1-5 not invoked on fork.
    for (let i = 1; i <= 5; i++) {
      expect(forkReg.count(`s${i}`)).toBe(0);
    }
  });

  // 2. FULL vs FORK with MODIFIED prompt at step 6: differs ONLY in step 6.
  it('full vs fork with modified step-6 prompt: steps 1-5 identical, step 6+ differ, step-6 input identical', async () => {
    const c = client();
    const base = chan('mod');
    const fork = `${base}-forkmod`;

    const fullLive: JsonObject = { topic: 'T' };
    const fullSession = new AgentSession(c, base, { stateFn: () => ({ ...fullLive }) });
    await fullSession.start();
    await runPipeline(fullSession, new Registry(), fullLive);
    await fullSession.complete();
    const fullState = await stateOf(c, base);

    const forkLive: JsonObject = { topic: 'T' };
    const forkSess = await AgentSession.resume(c, base, {
      forkAtStep: FORK_STEP,
      name: fork,
      stateFn: () => ({ ...forkLive }),
    });
    Object.assign(forkLive, forkSess.inheritedState ?? {});
    await forkSess.start();
    await runPipeline(forkSess, new Registry(), forkLive, { fromStep: FORK_STEP + 1, style: 'aggressive' });
    await forkSess.complete();
    const forkState = await stateOf(c, fork);

    // Steps 1-5 identical (inherited).
    for (let i = 1; i <= 5; i++) {
      expect(forkState[`s${i}_out`]).toBe(fullState[`s${i}_out`]);
    }
    // Step 6 DIFFERS (the modified prompt).
    expect(forkState['s6_out']).not.toBe(fullState['s6_out']);
    // Steps 7-9 differ transitively (they depend on s6).
    expect(forkState['s7_out']).not.toBe(fullState['s7_out']);
    expect(forkState['s9_out']).not.toBe(fullState['s9_out']);
    // But step 6's INPUT (the inherited context) is identical to the full run.
    const baseEvents = await c.replay(base, { limit: 500 });
    const forkEvents = await c.replay(fork, { limit: 500 });
    const fullS6 = baseEvents.find((e) => e.eventType === 'step.6');
    const forkS6 = forkEvents.find((e) => e.eventType === 'step.6');
    expect(fullS6).toBeDefined();
    expect(forkS6).toBeDefined();
    const fullInp = (fullS6!.payload as JsonObject)['input'] as JsonObject;
    const forkInp = (forkS6!.payload as JsonObject)['input'] as JsonObject;
    for (let i = 1; i <= 5; i++) {
      expect(forkInp[`s${i}_out`]).toBe(fullInp[`s${i}_out`]);
    }
  });

  // 3. Edge cases.
  it('fork at every step inherits the right prefix of state', async () => {
    const c = client();
    const base = chan('edge-a');
    const live: JsonObject = { topic: 'T' };
    const session = new AgentSession(c, base, { stateFn: () => ({ ...live }) });
    await session.start();
    await runPipeline(session, new Registry(), live);
    await session.complete();
    const full = await stateOf(c, base);

    for (let at = 1; at <= 9; at++) {
      const fname = `${base}-f${at}`;
      const fs = await AgentSession.resume(c, base, { forkAtStep: at, name: fname, stateFn: () => ({}) });
      const fstate = fs.inheritedState ?? {};
      // Fork at step N inherits steps 1..N.
      for (let i = 1; i <= at; i++) {
        expect(fstate[`s${i}_out`]).toBe(full[`s${i}_out`]);
      }
      // It does NOT inherit steps after N.
      for (let i = at + 1; i <= 9; i++) {
        expect(fstate[`s${i}_out`]).toBeUndefined();
      }
      expect(fs.stepCountValue).toBe(at);
    }
  });

  it('fork of a fork inherits the deepest state', async () => {
    const c = client();
    const base = chan('edge-b');
    const f1 = `${base}-f1`;
    const f2 = `${base}-f2`;
    const live: JsonObject = { topic: 'T' };
    const session = new AgentSession(c, base, { stateFn: () => ({ ...live }) });
    await session.start();
    await runPipeline(session, new Registry(), live);
    await session.complete();

    // f1: fork at 3, run steps 4-9.
    let l1: JsonObject = {};
    const s1 = await AgentSession.resume(c, base, { forkAtStep: 3, name: f1, stateFn: () => ({ ...l1 }) });
    l1 = { ...(s1.inheritedState ?? {}) };
    await s1.start();
    await runPipeline(s1, new Registry(), l1, { fromStep: 4 });
    await s1.complete();

    // f2: fork f1 at 6, run steps 7-9.
    let l2: JsonObject = {};
    const s2 = await AgentSession.resume(c, f1, { forkAtStep: 6, name: f2, stateFn: () => ({ ...l2 }) });
    l2 = { ...(s2.inheritedState ?? {}) };
    expect(l2['s1_out']).toBe(live['s1_out']);
    expect(l2['s6_out']).toBe(l1['s6_out']);
    expect(s2.stepCountValue).toBe(6);
    await s2.start();
    await runPipeline(s2, new Registry(), l2, { fromStep: 7 });
    await s2.complete();

    const types = await typesOf(c, f2);
    expect(types[0]).toBe('fork.started');
    for (const t of ['step.1', 'step.2', 'step.3', 'step.4', 'step.5', 'step.6']) {
      expect(types).not.toContain(t);
    }
  });

  it('forking an event-only channel is strict by default; approximate falls back', async () => {
    const c = client();
    const base = chan('edge-c');
    const live: JsonObject = { topic: 'T' };
    // No stateFn → no snapshots saved.
    const session = new AgentSession(c, base);
    await session.start();
    await runPipeline(session, new Registry(), live);
    await session.complete();

    // Strict default: no snapshot at the boundary → clear error.
    await expect(
      AgentSession.resume(c, base, { forkAtStep: 3, name: `${base}-f`, stateFn: () => ({}) }),
    ).rejects.toBeInstanceOf(NoRestorableCheckpointError);

    // Approximate mode: falls back to the latest state instead of erroring.
    const fs = await AgentSession.resume(c, base, {
      forkAtStep: 3,
      name: `${base}-f-approx`,
      stateFn: () => ({}),
      boundaryMode: 'approximate',
    });
    expect(fs.stepCountValue).toBe(3);
  });

  it('two concurrent forks from the same base are independent', async () => {
    const c = client();
    const base = chan('edge-d');
    const live: JsonObject = { topic: 'T' };
    const session = new AgentSession(c, base, { stateFn: () => ({ ...live }) });
    await session.start();
    await runPipeline(session, new Registry(), live);
    await session.complete();

    const faName = `${base}-fa`;
    const fbName = `${base}-fb`;
    let a: JsonObject = {};
    let b: JsonObject = {};
    const f1 = await AgentSession.resume(c, base, { forkAtStep: 5, name: faName, stateFn: () => ({ ...a }) });
    a = { ...(f1.inheritedState ?? {}) };
    const f2 = await AgentSession.resume(c, base, { forkAtStep: 5, name: fbName, stateFn: () => ({ ...b }) });
    b = { ...(f2.inheritedState ?? {}) };

    // Both inherited the same step-5 state independently.
    for (let i = 1; i <= 5; i++) {
      expect(a[`s${i}_out`]).toBe(b[`s${i}_out`]);
    }

    // Different step-6 styles → independent evolution.
    await f1.start();
    await runPipeline(f1, new Registry(), a, { fromStep: 6, style: 'aggressive' });
    await f1.complete();
    await f2.start();
    await runPipeline(f2, new Registry(), b, { fromStep: 6, style: 'neutral' });
    await f2.complete();

    const fa = await stateOf(c, faName);
    const fb = await stateOf(c, fbName);
    expect(fa['s6_out']).not.toBe(fb['s6_out']);
    expect(fa['s1_out']).toBe(fb['s1_out']);
  });

  it('re-forking the same name is idempotent (returns the original fork)', async () => {
    const c = client();
    const base = chan('edge-e');
    const live: JsonObject = { topic: 'T' };
    const session = new AgentSession(c, base, { stateFn: () => ({ ...live }) });
    await session.start();
    await runPipeline(session, new Registry(), live);
    await session.complete();

    const name = `${base}-idem`;
    const f1 = await AgentSession.resume(c, base, { forkAtStep: 5, name, stateFn: () => ({}) });
    const f2 = await AgentSession.resume(c, base, { forkAtStep: 5, name, stateFn: () => ({}) });
    // Both resolve to the same channel; the server is idempotent.
    expect(f1.sessionName).toBe(name);
    expect(f2.sessionName).toBe(name);
  });

  it('resume on a completed session raises SessionCompletedError', async () => {
    const c = client();
    const base = chan('edge-f');
    const live: JsonObject = { topic: 'T' };
    const session = new AgentSession(c, base, { stateFn: () => ({ ...live }) });
    await session.start();
    await runPipeline(session, new Registry(), live);
    await session.complete();

    await expect(AgentSession.resume(c, base, {})).rejects.toBeInstanceOf(SessionCompletedError);
  });

  it('resume on an unknown channel raises SessionError', async () => {
    const c = client();
    await expect(AgentSession.resume(c, `${chan('edge-g')}-does-not-exist`, {})).rejects.toBeInstanceOf(
      SessionError,
    );
  });

  it('nested state consistency: fork inherits exactly the parent snapshot at the boundary', async () => {
    const c = client();
    const base = chan('edge-h');
    const live: JsonObject = { topic: 'T' };
    const session = new AgentSession(c, base, { stateFn: () => ({ ...live }) });
    await session.start();
    await runPipeline(session, new Registry(), live);
    await session.complete();
    const full = await stateOf(c, base);

    const fs = await AgentSession.resume(c, base, { forkAtStep: 7, name: `${base}-deep`, stateFn: () => ({}) });
    const inherited = fs.inheritedState ?? {};
    // Fork at step 7 inherits steps 1..7 exactly; steps 8-9 do not exist yet.
    for (let i = 1; i <= 7; i++) {
      expect(inherited[`s${i}_out`]).toBe(full[`s${i}_out`]);
    }
    for (let i = 8; i <= 9; i++) {
      expect(inherited[`s${i}_out`]).toBeUndefined();
    }
    expect(inherited['s7_out']).toBe(full['s7_out']);
  });

  it('forking a raw channel outside AgentSession: replay-fallback + strict/approximate boundary policy', async () => {
    const c = client();
    const base = chan('edge-i');
    const live: JsonObject = { topic: 'T' };
    const events: Event[] = [];
    for (let i = 0; i < 5; i++) {
      events.push(await c.record(base, `ev.${i}`, { i }, { actor: 'raw' }));
    }
    await c.saveState(base, events[events.length - 1]!.cursor, live);

    // The only snapshot is at cursor 5; forking at step 3 (cursor 3) has no
    // boundary snapshot → strict default raises.
    await expect(
      AgentSession.resume(c, base, { forkAtStep: 3, name: `${base}-fork`, stateFn: () => ({}) }),
    ).rejects.toBeInstanceOf(NoRestorableCheckpointError);

    // Approximate mode: falls back to the latest state (cursor 5's snapshot).
    // The fallback resolves BEYOND the requested boundary, so the SDK refuses
    // to prime with the contaminated state — inheritedState is undefined.
    const fs = await AgentSession.resume(c, base, {
      forkAtStep: 3,
      name: `${base}-fork-approx`,
      stateFn: () => ({}),
      boundaryMode: 'approximate',
    });
    expect(fs.stepCountValue).toBe(3);
    expect(fs.inheritedState).toBeUndefined();
  });

  it('fork at a step beyond the recorded pipeline raises SessionError', async () => {
    const c = client();
    const base = chan('edge-j');
    const live: JsonObject = { topic: 'T' };
    const session = new AgentSession(c, base, { stateFn: () => ({ ...live }) });
    await session.start();
    await runPipeline(session, new Registry(), live); // 9 steps
    await session.complete();

    await expect(
      AgentSession.resume(c, base, { forkAtStep: 10, name: `${base}-f10`, stateFn: () => ({}) }),
    ).rejects.toBeInstanceOf(SessionError);
  });

  it('snapshotInterval>1: strict fork on a snapshotless step raises; approximate falls back', async () => {
    const c = client();
    const base = chan('edge-k');
    const live: JsonObject = { topic: 'T' };
    // Save snapshots every 3 steps: steps 3, 6, 9 have snapshots.
    const session = new AgentSession(c, base, { stateFn: () => ({ ...live }), snapshotInterval: 3 });
    await session.start();
    await runPipeline(session, new Registry(), live);
    await session.complete();

    // Fork at step 2 (no snapshot at/before cursor 2) → strict raises.
    await expect(
      AgentSession.resume(c, base, { forkAtStep: 2, name: `${base}-f2`, stateFn: () => ({}) }),
    ).rejects.toBeInstanceOf(NoRestorableCheckpointError);

    // Approximate mode: falls back to the latest state. The fallback resolves
    // BEYOND the requested boundary → the SDK refuses the contaminated state.
    const fs = await AgentSession.resume(c, base, {
      forkAtStep: 2,
      name: `${base}-f2-approx`,
      stateFn: () => ({}),
      boundaryMode: 'approximate',
    });
    expect(fs.stepCountValue).toBe(2);
    expect(fs.inheritedState).toBeUndefined();
  });

  it('lineage_only fork: no state copy, restorable=false, cursor 0, parent lineage recorded', async () => {
    const c = client();
    const base = chan('edge-m');
    const live: JsonObject = { topic: 'T' };
    // Event-only channel: no stateFn → zero snapshots saved.
    const session = new AgentSession(c, base);
    await session.start();
    await runPipeline(session, new Registry(), live);
    await session.complete();

    const fs = await AgentSession.resume(c, base, {
      forkAtStep: 5,
      name: `${base}-lo`,
      stateFn: () => ({}),
      boundaryMode: 'lineage_only',
    });
    expect(fs.stepCountValue).toBe(5);
    expect(fs.boundaryRestorable).toBe(false);
    expect(fs.resolvedBoundaryCursor).toBe(0);
    expect(fs.inheritedState).toBeUndefined();

    // The fork channel exists, is replayable, and its metadata records the
    // lineage to the parent.
    const types = await typesOf(c, fs.channelId);
    expect(types.length).toBeGreaterThan(0);
    const meta = await c.getChannelMetadata(fs.channelId);
    expect(meta).toBeDefined();
    expect(meta!.parentChannelId).toBe(base);

    // Stepping the fork works and continues at step 6 (no inherited prefix).
    await fs.start();
    await fs.step('step.6', { input: {}, output: { s6_out: 'refined' }, context: {} });
    const post = await typesOf(c, fs.channelId);
    expect(post).toContain('step.6');
  });

  it('forking a fork at an inherited step resolves against the ancestor that recorded it', async () => {
    const c = client();
    const base = chan('edge-l');
    const f1 = `${base}-f1`;
    const f2 = `${base}-f2`;
    const live: JsonObject = { topic: 'T' };
    const session = new AgentSession(c, base, { stateFn: () => ({ ...live }) });
    await session.start();
    await runPipeline(session, new Registry(), live);
    await session.complete();
    const full = await stateOf(c, base);

    // f1: fork at step 3 (inherits steps 1-3), then run steps 4-9.
    let l1: JsonObject = {};
    const s1 = await AgentSession.resume(c, base, { forkAtStep: 3, name: f1, stateFn: () => ({ ...l1 }) });
    l1 = { ...(s1.inheritedState ?? {}) };
    await s1.start();
    await runPipeline(s1, new Registry(), l1, { fromStep: 4 });
    await s1.complete();

    // f2: fork f1 at step 2 — an INHERITED step. The correct state is on the
    // root (base), and the fork boundary must be the root's step-2 cursor.
    let l2: JsonObject = {};
    const s2 = await AgentSession.resume(c, f1, { forkAtStep: 2, name: f2, stateFn: () => ({ ...l2 }) });
    l2 = { ...(s2.inheritedState ?? {}) };
    // f2 must inherit steps 1-2 from the ROOT, not f1's own first events.
    expect(l2['s1_out']).toBe(full['s1_out']);
    expect(l2['s2_out']).toBe(full['s2_out']);
    // It must NOT accidentally carry f1's later steps (4+) or even step 3.
    expect(l2['s3_out']).toBeUndefined();
    expect(l2['s4_out']).toBeUndefined();
    expect(s2.stepCountValue).toBe(2);
  });
});
