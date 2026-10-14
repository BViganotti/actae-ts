/**
 * Actae TypeScript SDK — LLM-judged fork-fidelity suite.
 *
 * A feature-parity port of the Python suite
 * (`examples/fork_fidelity_lib.py` + `fork_fidelity_scenarios.py` +
 * `fork_fidelity_suite.py`): for each of five unrelated pipelines we run a
 * baseline, a full no-fork re-run, and a fork at step N on a real Actae
 * server. A judge LLM (DeepSeek, temperature 0) scores semantic equivalence
 * of the TAIL outputs, and the suite asserts:
 *
 *   1. fork_fidelity  >= threshold             (fork output equivalent to baseline)
 *   2. fork_fidelity  >= rerun_fidelity - tolerance   (forking not worse than re-running)
 *
 * The inherited prefix (steps 1..N) is verified byte-identical, and the fork
 * receipt must report a restorable boundary, equal requested/resolved
 * cursors and a non-empty source-state SHA-256 (verified loudly).
 *
 * The judge is the ONLY fidelity metric; dry-run mode uses a deterministic
 * char-ratio judge + deterministic pipeline output (plumbing only, no API
 * key). The five scenario prompts are byte-for-byte copies of the Python
 * definitions — the same scenarios run through every SDK.
 *
 * Two fork adapters (--adapter):
 *   session    — AgentSession.fork (default; the Python/Go port's machinery)
 *   langgraph  — a real @langchain/langgraph StateGraph (one node per step,
 *                single last-wins `acc` channel) compiled with
 *                ActaeCheckpointSaver; the fork uses
 *                saver.forkThread(config, {newThreadId, reason}) at the
 *                newest checkpoint holding exactly fork_at_step step keys.
 *                The report carries adapter: "langgraph" plus a per-scenario
 *                `langgraph` block (thread, forkChannel, forkedCheckpointId,
 *                receipt) and the report file gets the -langgraph suffix.
 *
 * Run against dev mode:
 *   cd actae && cargo run        # Actae on :8002
 *   cd sdks/typescript && npm run build        # dist/ must exist
 *   ACTAE_URL=http://localhost:8002 ACTAE_API_KEY=sk-dev-0000000000000000000000 \
 *     node examples/fidelity/index.ts --dry-run
 *   ... --adapter langgraph --dry-run          # LangGraph native fork path
 *
 * Real evidence run (DEEPSEEK_API_KEY in actae/.env or the environment):
 *   node examples/fidelity/index.ts --repeats 3 --judge-reps 1
 *   node examples/fidelity/index.ts --adapter langgraph --repeats 3 --judge-reps 1
 *
 * Exit code 0 (all scenarios passed) or 1 (any failed / hard error). Reports
 * go to <report-dir>/fork-fidelity[-langgraph]-ts-<run-id>.json.
 */

import { randomBytes } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  ActaeClient,
  AgentSession,
  APIError,
  ConnectionError,
  newClientFromEnv,
  RateLimitError,
  SessionError,
} from '../../dist/index.js';
import type { ForkReceipt, JsonObject, JsonValue } from '../../dist/index.js';
import { StateGraph } from '@langchain/langgraph';
import type { StateSnapshot } from '@langchain/langgraph';
import { ActaeCheckpointSaver } from '../../dist/adapters/langgraph.js';

// ---------------------------------------------------------------------------
// Domain model
// ---------------------------------------------------------------------------

class FidelityError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'FidelityError';
    Object.setPrototypeOf(this, FidelityError.prototype);
  }
}

interface StepDef {
  key: string;
  system: string;
  user: (state: Record<string, string>) => string;
  temperature: number;
  max_tokens: number;
}

interface Scenario {
  name: string;
  description: string;
  topic: string;
  steps: StepDef[];
  fork_at_step: number;
  input_text?: string;
  params: Record<string, string>;
}

function validateScenario(sc: Scenario): void {
  if (!sc.name || !/^[a-z0-9][a-z0-9_-]*$/.test(sc.name)) {
    throw new FidelityError(`scenario name ${sc.name} must be a lowercase slug`);
  }
  if (sc.steps.length === 0) {
    throw new FidelityError(`scenario ${sc.name}: no steps`);
  }
  const keys = sc.steps.map((s) => s.key);
  if (new Set(keys).size !== keys.length) {
    throw new FidelityError(`scenario ${sc.name}: duplicate step keys ${keys.join(', ')}`);
  }
  if (!(1 <= sc.fork_at_step && sc.fork_at_step < sc.steps.length)) {
    throw new FidelityError(
      `scenario ${sc.name}: fork_at_step=${sc.fork_at_step} must be in ` +
        `[1, ${sc.steps.length - 1}] (needs ≥1 inherited step and ≥1 tail step)`,
    );
  }
}

function inheritedKeys(sc: Scenario): string[] {
  return sc.steps.slice(0, sc.fork_at_step).map((s) => s.key);
}

function tailKeys(sc: Scenario): string[] {
  return sc.steps.slice(sc.fork_at_step).map((s) => s.key);
}

// ---------------------------------------------------------------------------
// Judge — the ONLY fidelity metric
// ---------------------------------------------------------------------------

const JUDGE_SYSTEM = `You are an impartial fidelity judge for agent-pipeline experiments.

You compare TWO OUTPUTS of the same multi-step pipeline and score how
semantically equivalent they are. A CANDIDATE output is judged against a
REFERENCE output.

Rules:
- Judge FACTS, CONCLUSIONS and COVERAGE — never wording, style, length or
  formatting. Different phrasing, ordering or emphasis is NORMAL (the model
  samples) and must not lower the score.
- When a pipeline step is asked to PROPOSE or INVENT specific values (prices,
  targets, estimates, timelines), different proposed values for the SAME item
  are NOT factual contradictions — both are valid samples. What matters is
  whether the candidate covered the same set of items and its reasoning
  matches. Do NOT penalize differing invented numbers; DO penalize a missing
  item, a genuinely wrong one, or reversed reasoning.
- Equivalent = same facts and conclusions, same coverage of key points.
- Missing, added, or contradicted KEY facts lower the score.
- Return ONLY a JSON object with exactly these fields:
  {"score": <integer 1-10>, "verdict": "equivalent"|"minor_differences"|"substantive_differences", "rationale": "<1-2 sentences>"}
- score 9-10: equivalent; 7-8: minor differences, no substantive change;
  4-6: some key points missing or wrong; 1-3: substantially different.`;

function buildJudgeUser(reference: string, candidate: string): string {
  return (
    'REFERENCE output:\n' +
    '--------------------\n' +
    `${reference}\n\n` +
    'CANDIDATE output:\n' +
    '--------------------\n' +
    `${candidate}\n`
  );
}

function parseJudgeJson(text: string): { score: number; verdict: string; rationale: string } {
  let raw = (text || '').trim();
  if (raw.startsWith('```')) {
    raw = raw.replace(/^```(?:json)?\s*/, '').replace(/`+$/, '').trim();
  }
  let obj: JsonObject;
  try {
    obj = JSON.parse(raw) as JsonObject;
  } catch {
    const mScore = raw.match(/"score"\s*:\s*(\d{1,2})/);
    if (!mScore) {
      throw new FidelityError(`judge reply unparseable: ${text.slice(0, 200)}`);
    }
    const mVerdict = raw.match(/"verdict"\s*:\s*"([^"]+)"/);
    obj = {
      score: parseInt(mScore[1]!, 10),
      verdict: mVerdict ? mVerdict[1]! : 'unknown',
      rationale: raw.slice(0, 300),
    };
  }
  const score = obj['score'];
  if (typeof score !== 'number' || !Number.isInteger(score) || !(1 <= score && score <= 10)) {
    throw new FidelityError(`judge score out of range: ${String(score)} (${text.slice(0, 200)})`);
  }
  const verdict = typeof obj['verdict'] === 'string' ? (obj['verdict'] as string) : 'unknown';
  const rationale = typeof obj['rationale'] === 'string' ? (obj['rationale'] as string) : '';
  return { score, verdict, rationale };
}

interface JudgeSample {
  score: number;
  verdict: string;
  rationale: string;
}

interface FidelityComparison {
  score: number;
  verdict: string;
  rationale: string;
  samples: JudgeSample[];
}

function verdictFor(score: number): string {
  if (score >= 9.0) return 'equivalent';
  if (score >= 7.0) return 'minor_differences';
  if (score >= 4.0) return 'some_substantive_differences';
  return 'substantively_different';
}

function comparisonToDict(c: FidelityComparison): JsonObject {
  return {
    score: round2(c.score),
    verdict: c.verdict,
    rationale: c.rationale,
    samples: c.samples.map((s) => ({ score: s.score, verdict: s.verdict, rationale: s.rationale })),
  };
}

interface Judge {
  score(reference: string, candidate: string): Promise<FidelityComparison>;
}

// ---------------------------------------------------------------------------
// DeepSeek client (plain fetch, no dependencies)
// ---------------------------------------------------------------------------

interface ChatMessage {
  role: string;
  content: string;
}

interface ChatResult {
  content: string;
  prompt_tokens: number;
  completion_tokens: number;
}

class DeepSeekClient {
  private apiKey: string;
  private readonly maxAttempts = 5;

  constructor(apiKey: string) {
    this.apiKey = apiKey;
  }

  async complete(opts: {
    model: string;
    messages: ChatMessage[];
    temperature: number;
    max_tokens: number;
  }): Promise<ChatResult> {
    const url = 'https://api.deepseek.com/chat/completions';
    let lastErr: Error | undefined;
    for (let attempt = 0; attempt < this.maxAttempts; attempt++) {
      try {
        const res = await fetch(url, {
          method: 'POST',
          headers: {
            Authorization: `Bearer ${this.apiKey}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            model: opts.model,
            messages: opts.messages,
            temperature: opts.temperature,
            max_tokens: opts.max_tokens,
          }),
        });
        if (res.status === 429 || res.status >= 500) {
          lastErr = new Error(`DeepSeek HTTP ${res.status}`);
          const retryAfter = Number(res.headers.get('retry-after') ?? '') || 0;
          await sleep(Math.min((retryAfter || 5 * 2 ** attempt) * 1000, 30000));
          continue;
        }
        if (!res.ok) {
          const detail = (await res.text()).slice(0, 200);
          throw new FidelityError(`DeepSeek HTTP ${res.status}: ${detail}`);
        }
        const body = (await res.json()) as {
          choices?: { message?: { content?: string } }[];
          usage?: { prompt_tokens?: number; completion_tokens?: number };
        };
        const content = body.choices?.[0]?.message?.content ?? '';
        const usage = body.usage ?? {};
        return {
          content,
          prompt_tokens: usage.prompt_tokens ?? 0,
          completion_tokens: usage.completion_tokens ?? 0,
        };
      } catch (err) {
        if (err instanceof FidelityError) throw err;
        lastErr = err as Error;
        await sleep(Math.min(5 * 2 ** attempt, 30) * 1000);
      }
    }
    throw new FidelityError(`DeepSeek request failed after ${this.maxAttempts} attempts: ${lastErr?.message ?? 'unknown'}`);
  }
}

class LLMJudge implements Judge {
  private chat: DeepSeekClient;
  private opts: { model: string; reps: number; maxTokens: number; retries: number };

  constructor(chat: DeepSeekClient, opts: { model: string; reps: number; maxTokens?: number; retries?: number }) {
    this.chat = chat;
    this.opts = { model: opts.model, reps: opts.reps, maxTokens: opts.maxTokens ?? 256, retries: opts.retries ?? 3 };
  }

  async score(reference: string, candidate: string): Promise<FidelityComparison> {
    const samples: JudgeSample[] = [];
    for (let i = 0; i < this.opts.reps; i++) {
      samples.push(await this.sample(reference, candidate));
    }
    const score = samples.reduce((a, s) => a + s.score, 0) / samples.length;
    const rationale = samples.map((s) => s.rationale).join(' | ');
    return { score, verdict: verdictFor(score), rationale, samples };
  }

  private async sample(reference: string, candidate: string): Promise<JudgeSample> {
    let lastErr: Error | undefined;
    for (let attempt = 0; attempt < this.opts.retries; attempt++) {
      try {
        const resp = await this.chat.complete({
          model: this.opts.model,
          messages: [
            { role: 'system', content: JUDGE_SYSTEM },
            { role: 'user', content: buildJudgeUser(reference, candidate) },
          ],
          temperature: 0.0,
          max_tokens: this.opts.maxTokens,
        });
        const parsed = parseJudgeJson(resp.content);
        return { score: parsed.score, verdict: parsed.verdict, rationale: parsed.rationale };
      } catch (err) {
        lastErr = err as Error;
        if (!(err instanceof FidelityError)) throw err;
        if (attempt === this.opts.retries - 1) {
          throw new FidelityError(`judge failed after ${this.opts.retries} attempts: ${(err as Error).message}`);
        }
        await sleep(1000);
      }
    }
    throw new FidelityError(`judge failed after ${this.opts.retries} attempts: ${lastErr?.message ?? 'unknown'}`);
  }
}

/** difflib.SequenceMatcher-style char ratio (longest-match + recursion). */
function sequenceMatcherRatio(a: string, b: string): number {
  if (a.length === 0 && b.length === 0) return 1.0;
  let total = 0;

  function findLongestMatch(alo: number, ahi: number, blo: number, bhi: number): { i: number; j: number; size: number } {
    let bestI = alo;
    let bestJ = blo;
    let bestSize = 0;
    const j2len = new Map<number, number>();
    for (let i = alo; i < ahi; i++) {
      const newj2len = new Map<number, number>();
      for (let j = blo; j < bhi; j++) {
        if (a[i] === b[j]) {
          const k = (j2len.get(j - 1) ?? 0) + 1;
          newj2len.set(j, k);
          if (k > bestSize) {
            bestI = i - k + 1;
            bestJ = j - k + 1;
            bestSize = k;
          }
        }
      }
      j2len.clear();
      for (const [j, k] of newj2len) j2len.set(j, k);
    }
    return { i: bestI, j: bestJ, size: bestSize };
  }

  function rec(alo: number, ahi: number, blo: number, bhi: number): void {
    const m = findLongestMatch(alo, ahi, blo, bhi);
    if (m.size > 0) {
      if (alo < m.i && blo < m.j) rec(alo, m.i, blo, m.j);
      total += m.size;
      const iEnd = m.i + m.size;
      const jEnd = m.j + m.size;
      if (iEnd < ahi && jEnd < bhi) rec(iEnd, ahi, jEnd, bhi);
    }
  }

  rec(0, a.length, 0, b.length);
  return (2 * total) / (a.length + b.length);
}

class DeterministicJudge implements Judge {
  async score(reference: string, candidate: string): Promise<FidelityComparison> {
    const ratio = sequenceMatcherRatio(reference, candidate);
    const score = Math.round(1 + ratio * 9);
    const verdict = verdictFor(score);
    const rationale = `dry-run deterministic (char-ratio ${ratio.toFixed(2)})`;
    const sample: JudgeSample = { score, verdict, rationale };
    return { score, verdict, rationale, samples: [sample] };
  }
}

// ---------------------------------------------------------------------------
// Token accounting (StepRegistry port)
// ---------------------------------------------------------------------------

class StepRegistry {
  private calls: Record<string, number> = {};
  private tokens: Record<string, { prompt: number; completion: number }> = {};
  private elapsedMs: Record<string, number> = {};

  record(step: string, promptTokens: number, completionTokens: number, elapsedMs: number): void {
    if (promptTokens < 0 || completionTokens < 0 || elapsedMs < 0) {
      throw new FidelityError(
        `negative usage for ${step}: prompt=${promptTokens} completion=${completionTokens} elapsed_ms=${elapsedMs}`,
      );
    }
    const acc = this.tokens[step] ?? (this.tokens[step] = { prompt: 0, completion: 0 });
    acc.prompt += promptTokens;
    acc.completion += completionTokens;
    this.elapsedMs[step] = (this.elapsedMs[step] ?? 0) + elapsedMs;
    this.calls[step] = (this.calls[step] ?? 0) + 1;
  }

  stepTokens(step: string): number {
    const acc = this.tokens[step];
    return acc ? acc.prompt + acc.completion : 0;
  }

  totalCalls(): number {
    return Object.values(this.calls).reduce((a, b) => a + b, 0);
  }

  totalTokens(): number {
    return Object.values(this.tokens).reduce((a, t) => a + (t.prompt + t.completion), 0);
  }
}

// ---------------------------------------------------------------------------
// Pipeline execution (baseline / re-run / fork)
// ---------------------------------------------------------------------------

interface TraceEntry {
  step_number: number;
  key: string;
  system: string;
  user: string;
  temperature: number;
  max_tokens: number;
  output: string;
}

interface RunOpts {
  chat: DeepSeekClient | null;
  model: string;
  dryRun: boolean;
}

async function llmText(
  chat: DeepSeekClient | null,
  opts: RunOpts & { stepKey: string; system: string; user: string; temperature: number; maxTokens: number },
): Promise<{ content: string; promptTokens: number; completionTokens: number }> {
  if (opts.dryRun) {
    const content = `[dry-run:${opts.stepKey}] ${opts.system.slice(0, 80)}`;
    return {
      content,
      promptTokens: 60 + Math.floor(opts.user.length / 4),
      completionTokens: 20 + Math.floor(content.length / 4),
    };
  }
  const r = await chat!.complete({
    model: opts.model,
    messages: [
      { role: 'system', content: opts.system },
      { role: 'user', content: opts.user },
    ],
    temperature: opts.temperature,
    max_tokens: opts.maxTokens,
  });
  return { content: r.content.trim(), promptTokens: r.prompt_tokens, completionTokens: r.completion_tokens };
}

function initialLive(sc: Scenario): Record<string, string> {
  const live: Record<string, string> = { topic: sc.topic };
  if (sc.input_text) live['_input'] = sc.input_text;
  return live;
}

function _s(state: Record<string, string>, keys: string[]): string {
  const parts: string[] = [];
  for (const k of keys) {
    const v = state[k];
    if (v) parts.push(v);
  }
  return parts.join('\n\n');
}

async function runSteps(
  session: AgentSession,
  sc: Scenario,
  registry: StepRegistry,
  live: Record<string, string>,
  opts: RunOpts & { fromIdx: number; trace: TraceEntry[] },
): Promise<void> {
  for (let i = opts.fromIdx; i < sc.steps.length; i++) {
    const step = sc.steps[i]!;
    const user = step.user(live);
    const { content, promptTokens, completionTokens } = await llmText(opts.chat, {
      ...opts,
      stepKey: step.key,
      system: step.system,
      user,
      temperature: step.temperature,
      maxTokens: step.max_tokens,
    });
    registry.record(step.key, promptTokens, completionTokens, 0);
    live[step.key] = content;
    opts.trace.push({
      step_number: i + 1,
      key: step.key,
      system: step.system,
      user,
      temperature: step.temperature,
      max_tokens: step.max_tokens,
      output: content,
    });
    await session.step(`step.${i + 1}.${step.key}`, {
      input: user,
      output: { tokens: registry.stepTokens(step.key) },
      context: { [step.key]: content },
    });
  }
}

interface PipelineResult {
  state: JsonObject;
  registry: StepRegistry;
  trace: TraceEntry[];
}

async function runPipeline(
  actae: ActaeClient,
  sc: Scenario,
  channel: string,
  opts: RunOpts,
): Promise<PipelineResult> {
  validateScenario(sc);
  const registry = new StepRegistry();
  const trace: TraceEntry[] = [];
  const live = initialLive(sc);
  const session = new AgentSession(actae, channel, {
    displayName: `Fidelity ${sc.name}`,
    stateFn: () => ({ ...live }),
    params: { ...sc.params, run: 'fidelity-pipeline' },
  });
  await session.start();
  try {
    await runSteps(session, sc, registry, live, { ...opts, fromIdx: 0, trace });
  } finally {
    await session.complete();
  }
  return { state: { ...live }, registry, trace };
}

interface Channels {
  baseline: string;
  rerun: string;
  fork: string;
}

interface ForkProvenance {
  boundary_restorable: boolean;
  requested_boundary_cursor: number;
  resolved_boundary_cursor: number;
  source_state_version: number;
  source_state_sha256: string;
  reproducibility: string;
  inherited_state: JsonObject;
}

async function runFork(
  actae: ActaeClient,
  sc: Scenario,
  baseChannel: string,
  forkChannel: string,
  opts: RunOpts,
): Promise<PipelineResult & { provenance: ForkProvenance }> {
  validateScenario(sc);
  const registry = new StepRegistry();
  const trace: TraceEntry[] = [];
  const live = initialLive(sc);

  const session = await AgentSession.resume(actae, baseChannel, {
    forkAtStep: sc.fork_at_step,
    name: forkChannel,
    stateFn: () => ({ ...live }),
    params: { ...sc.params, run: 'fidelity-fork' },
  });
  const inherited = session.inheritedState ?? {};
  const provenance: ForkProvenance = {
    boundary_restorable: session.boundaryRestorable ?? false,
    requested_boundary_cursor: session.requestedBoundaryCursor,
    resolved_boundary_cursor: session.resolvedBoundaryCursor,
    source_state_version: session.sourceStateVersionValue,
    source_state_sha256: session.sourceStateSha256,
    reproducibility: session.reproducibility,
    inherited_state: inherited,
  };
  if (session.boundaryRestorable !== true) {
    throw new FidelityError(`${sc.name}: fork boundary is not restorable`);
  }
  if (session.requestedBoundaryCursor !== session.resolvedBoundaryCursor) {
    throw new FidelityError(
      `${sc.name}: requested cursor ${session.requestedBoundaryCursor} resolved ` +
        `to ${session.resolvedBoundaryCursor}`,
    );
  }
  if (!session.sourceStateSha256) {
    throw new FidelityError(`${sc.name}: fork receipt has no source-state fingerprint`);
  }
  for (const [k, v] of Object.entries(inherited)) live[k] = String(v);
  if (sc.input_text && !('_input' in live)) live['_input'] = sc.input_text;

  await session.start();
  try {
    await runSteps(session, sc, registry, live, { ...opts, fromIdx: sc.fork_at_step, trace });
  } finally {
    await session.complete();
  }
  return { state: { ...live }, registry, trace, provenance };
}

// ---------------------------------------------------------------------------
// LangGraph adapter pipeline (ActaeCheckpointSaver.forkThread)
// ---------------------------------------------------------------------------

type Adapter = 'session' | 'langgraph';

interface LangGraphDetail {
  thread: string;
  forkChannel: string;
  forkedCheckpointId: string;
  receipt: JsonObject;
}

interface LangGraphRunResult {
  state: Record<string, string>;
  registry: StepRegistry;
  trace: TraceEntry[];
  channel: string;
  graph: CompiledGraph;
  saver: ActaeCheckpointSaver;
}

/** Minimal structural shape of the compiled graph (strict-tsc friendly). */
interface CompiledGraph {
  invoke(input: unknown, config?: Record<string, unknown>): Promise<{ acc?: Record<string, string> }>;
  getStateHistory(config: Record<string, unknown>): AsyncIterableIterator<StateSnapshot>;
}

/** Loose structural builder over LangGraph's StateGraph (strict-tsc friendly:
 * avoids the framework's generic gymnastics while keeping the exact same
 * runtime call shape as the SDK's own tests). */
interface StateGraphBuilder {
  addNode(
    name: string,
    action: (state: { acc?: Record<string, string> }) => Promise<{ acc: Record<string, string> }>,
  ): StateGraphBuilder;
  addEdge(start: string, end: string): StateGraphBuilder;
  compile(options: unknown): CompiledGraph;
}

/** LangGraph root-thread config (checkpoint_ns undefined → ''). */
function lgCfg(threadId: string, extra: Record<string, unknown> = {}): Record<string, unknown> {
  return { configurable: { thread_id: threadId, ...extra } };
}

function countStepKeys(acc: Record<string, string> | undefined, sc: Scenario): number {
  if (!acc) return 0;
  const stepKeys = new Set(sc.steps.map((s) => s.key));
  return Object.keys(acc).filter((k) => stepKeys.has(k)).length;
}

/** Builds a linear StateGraph: START → n1 → n2 → … → nM → END, one node per
 * scenario step. State is a single last-wins channel `acc`; each node reads
 * `state.acc`, builds its prompt from prior step keys and writes back its own
 * key. */
function buildLangGraphGraph(
  saver: ActaeCheckpointSaver,
  sc: Scenario,
  opts: RunOpts & { trace: TraceEntry[]; registry: StepRegistry },
): CompiledGraph {
  const g = new StateGraph({
    channels: {
      acc: { reducer: (a: Record<string, string>, b: Record<string, string>) => b },
    },
  } as never) as unknown as StateGraphBuilder;
  sc.steps.forEach((step, i) => {
    const node = `n${i + 1}`;
    g.addNode(node, async (state: { acc?: Record<string, string> }) => {
      const acc = state.acc ?? {};
      const user = step.user(acc);
      const { content, promptTokens, completionTokens } = await llmText(opts.chat, {
        ...opts,
        stepKey: step.key,
        system: step.system,
        user,
        temperature: step.temperature,
        maxTokens: step.max_tokens,
      });
      opts.registry.record(step.key, promptTokens, completionTokens, 0);
      opts.trace.push({
        step_number: i + 1,
        key: step.key,
        system: step.system,
        user,
        temperature: step.temperature,
        max_tokens: step.max_tokens,
        output: content,
      });
      return { acc: { ...acc, [step.key]: content } };
    });
    if (i === 0) g.addEdge('__start__', node);
    else g.addEdge(`n${i}`, node);
  });
  g.addEdge(`n${sc.steps.length}`, '__end__');
  return g.compile({ checkpointer: saver });
}

/** Full pipeline run on a fresh thread: `graph.invoke(init, cfg(thread))`.
 * Final state is `res.acc` (topic + every step key, last-wins). */
async function runLangGraphFull(
  actae: ActaeClient,
  sc: Scenario,
  thread: string,
  channel: string,
  opts: RunOpts & { trace: TraceEntry[]; registry: StepRegistry },
): Promise<LangGraphRunResult> {
  const saver = new ActaeCheckpointSaver(actae, { channel });
  const graph = buildLangGraphGraph(saver, sc, { ...opts, trace: opts.trace, registry: opts.registry });
  const res = await graph.invoke({ acc: initialLive(sc) }, lgCfg(thread));
  const acc = res.acc ?? initialLive(sc);
  return { state: acc, registry: opts.registry, trace: opts.trace, channel: saver.channelForConfig(lgCfg(thread)), graph, saver };
}

function inheritedStateFromAcc(acc: Record<string, string>, sc: Scenario): JsonObject {
  const out: JsonObject = {};
  for (const k of inheritedKeys(sc)) if (acc[k] !== undefined) out[k] = acc[k];
  return out;
}

function provenanceFromReceipt(receipt: ForkReceipt, inherited: JsonObject): ForkProvenance {
  return {
    boundary_restorable: receipt.restorable,
    requested_boundary_cursor: receipt.requestedCursor,
    resolved_boundary_cursor: receipt.resolvedCursor,
    source_state_version: receipt.sourceStateVersion,
    source_state_sha256: receipt.sourceStateSha256 ?? '',
    reproducibility: receipt.reproducibility ?? '',
    inherited_state: inherited,
  };
}

async function runOneLangGraphRepeat(
  actae: ActaeClient,
  sc: Scenario,
  opts: RunOpts & { judge: Judge },
): Promise<RepeatRowData & { langgraph: LangGraphDetail }> {
  validateScenario(sc);
  const suffix = randomBytes(4).toString('hex');
  const channel = `fid-${sc.name}-${suffix}`;
  const thread = `t-${suffix}`;
  const rerunThread = `${thread}-rerun`;
  const forkThread = `${thread}-fork`;

  const baseTrace: TraceEntry[] = [];
  const baseRegistry = new StepRegistry();
  const base = await runLangGraphFull(actae, sc, thread, channel, { ...opts, trace: baseTrace, registry: baseRegistry });

  const rerunTrace: TraceEntry[] = [];
  const rerunRegistry = new StepRegistry();
  const rerun = await runLangGraphFull(actae, sc, rerunThread, channel, { ...opts, trace: rerunTrace, registry: rerunRegistry });

  // History is newest-first. The fork boundary is the newest checkpoint whose
  // acc holds exactly fork_at_step step keys (the moment right after step N
  // completed, before step N+1 runs — so resuming re-runs only the tail).
  const history: StateSnapshot[] = [];
  for await (const h of base.graph.getStateHistory(lgCfg(thread))) history.push(h);
  const boundary = history.find(
    (h) => countStepKeys((h.values as { acc?: Record<string, string> })?.acc, sc) === sc.fork_at_step,
  );
  if (!boundary) {
    throw new FidelityError(`${sc.name}: no checkpoint with exactly ${sc.fork_at_step} step keys`);
  }
  const boundaryId = boundary.config?.configurable?.checkpoint_id;
  if (typeof boundaryId !== 'string' || boundaryId === '') {
    throw new FidelityError(`${sc.name}: boundary checkpoint has no checkpoint_id`);
  }

  const forkCfg = await base.saver.forkThread(
    { configurable: { thread_id: thread, checkpoint_id: boundaryId } },
    { newThreadId: forkThread, reason: `fidelity fork at step ${sc.fork_at_step}` },
  );
  const forkChannel = base.saver.channelForConfig(forkCfg);

  const forkTrace: TraceEntry[] = [];
  const forkRegistry = new StepRegistry();
  const forkGraph = buildLangGraphGraph(base.saver, sc, { ...opts, trace: forkTrace, registry: forkRegistry });
  const forkRes = await forkGraph.invoke(null, forkCfg);
  const forkAcc = forkRes.acc ?? initialLive(sc);

  const receipt = await actae.getForkReceipt(forkChannel);
  if (receipt.restorable !== true) {
    throw new FidelityError(`${sc.name}: fork boundary is not restorable`);
  }
  if (receipt.requestedCursor !== receipt.resolvedCursor) {
    throw new FidelityError(
      `${sc.name}: requested cursor ${receipt.requestedCursor} resolved to ${receipt.resolvedCursor}`,
    );
  }
  if (!receipt.sourceStateSha256) {
    throw new FidelityError(`${sc.name}: fork receipt has no source-state fingerprint`);
  }

  const prefix = prefixIdentical(base.state as unknown as JsonObject, forkAcc as unknown as JsonObject, sc);
  const forkCmp = await opts.judge.score(tailText(base.state as unknown as JsonObject, sc), tailText(forkAcc as unknown as JsonObject, sc));
  const rerunCmp = await opts.judge.score(tailText(base.state as unknown as JsonObject, sc), tailText(rerun.state as unknown as JsonObject, sc));

  return {
    prefixOk: prefix.ok,
    prefixMismatches: prefix.mismatches,
    fork: forkCmp,
    rerun: rerunCmp,
    calls: baseRegistry.totalCalls() + rerunRegistry.totalCalls() + forkRegistry.totalCalls(),
    tokens: baseRegistry.totalTokens() + rerunRegistry.totalTokens() + forkRegistry.totalTokens(),
    channels: { baseline: base.channel, rerun: rerun.channel, fork: forkChannel },
    observations: {
      baseline: { state: base.state as unknown as JsonObject, trace: base.trace, tail: tailText(base.state as unknown as JsonObject, sc) },
      rerun: { state: rerun.state as unknown as JsonObject, trace: rerun.trace, tail: tailText(rerun.state as unknown as JsonObject, sc) },
      fork: { state: forkAcc as unknown as JsonObject, trace: forkTrace, tail: tailText(forkAcc as unknown as JsonObject, sc) },
    },
    forkProvenance: provenanceFromReceipt(receipt, inheritedStateFromAcc(forkAcc, sc)),
    langgraph: {
      thread,
      forkChannel,
      forkedCheckpointId: boundaryId,
      receipt: {
        fork_id: receipt.forkId,
        source_channel_id: receipt.sourceChannelId ?? null,
        child_channel_id: receipt.childChannelId,
        requested_cursor: receipt.requestedCursor,
        resolved_cursor: receipt.resolvedCursor,
        source_state_version: receipt.sourceStateVersion,
        source_state_sha256: receipt.sourceStateSha256 ?? '',
        restorable: receipt.restorable,
        replayed: receipt.replayed,
        reproducibility: receipt.reproducibility ?? null,
      },
    },
  };
}

// ---------------------------------------------------------------------------
// Comparison helpers (pure)
// ---------------------------------------------------------------------------

function tailText(state: JsonObject, sc: Scenario): string {
  const parts: string[] = [];
  for (const key of tailKeys(sc)) {
    const val = state[key];
    if (val) parts.push(`[${key}]\n${String(val)}`);
  }
  return parts.join('\n\n');
}

function prefixIdentical(
  baseState: JsonObject,
  forkState: JsonObject,
  sc: Scenario,
): { ok: boolean; mismatches: string[] } {
  const mismatches = inheritedKeys(sc).filter((k) => forkState[k] !== baseState[k]);
  return { ok: mismatches.length === 0, mismatches };
}

// ---------------------------------------------------------------------------
// Scenario evaluation + aggregation
// ---------------------------------------------------------------------------

interface Observations {
  baseline: { state: JsonObject; trace: TraceEntry[]; tail: string };
  rerun: { state: JsonObject; trace: TraceEntry[]; tail: string };
  fork: { state: JsonObject; trace: TraceEntry[]; tail: string };
}

interface RepeatRow {
  fork_fidelity: number;
  rerun_fidelity: number;
  prefix_identical: boolean;
  retried: boolean;
  channels: Channels;
  observations: Observations;
  fork_provenance: ForkProvenance;
  langgraph?: LangGraphDetail;
}

class ScenarioResult {
  passed: boolean;
  failReason: string | null;
  readonly name: string;
  readonly description: string;
  readonly topic: string;
  readonly stepCount: number;
  readonly forkAtStep: number;
  readonly inheritedSteps: string[];
  readonly tailSteps: string[];
  readonly prefixIdentical: boolean;
  readonly prefixMismatches: string[];
  readonly fork: FidelityComparison;
  readonly rerun: FidelityComparison;
  readonly repeats: RepeatRow[];
  readonly calls: number;
  readonly tokens: number;
  readonly channels: Channels[];
  readonly langgraph?: LangGraphDetail;

  constructor(
    name: string,
    description: string,
    topic: string,
    stepCount: number,
    forkAtStep: number,
    inheritedSteps: string[],
    tailSteps: string[],
    prefixIdentical: boolean,
    prefixMismatches: string[],
    fork: FidelityComparison,
    rerun: FidelityComparison,
    repeats: RepeatRow[],
    calls: number,
    tokens: number,
    channels: Channels[],
    langgraph?: LangGraphDetail,
  ) {
    this.name = name;
    this.description = description;
    this.topic = topic;
    this.stepCount = stepCount;
    this.forkAtStep = forkAtStep;
    this.inheritedSteps = inheritedSteps;
    this.tailSteps = tailSteps;
    this.prefixIdentical = prefixIdentical;
    this.prefixMismatches = prefixMismatches;
    this.fork = fork;
    this.rerun = rerun;
    this.repeats = repeats;
    this.calls = calls;
    this.tokens = tokens;
    this.channels = channels;
    this.langgraph = langgraph;
    this.passed = false;
    this.failReason = null;
  }

  toDict(): JsonObject {
    const dict: JsonObject = {
      name: this.name,
      description: this.description,
      topic: this.topic,
      step_count: this.stepCount,
      fork_at_step: this.forkAtStep,
      inherited_steps: this.inheritedSteps,
      tail_steps: this.tailSteps,
      prefix_identical: this.prefixIdentical,
      prefix_mismatches: this.prefixMismatches,
      fork_fidelity: comparisonToDict(this.fork),
      rerun_fidelity: comparisonToDict(this.rerun),
      repeats: this.repeats as unknown as JsonValue[],
      pass: this.passed,
      fail_reason: this.failReason,
      llm_calls: this.calls,
      tokens: this.tokens,
      channels: this.channels as unknown as JsonValue[],
    };
    if (this.langgraph) dict['langgraph'] = this.langgraph as unknown as JsonValue;
    return dict;
  }

  static fromParts(
    sc: Scenario,
    prefixOk: boolean,
    prefixMismatches: string[],
    fork: FidelityComparison,
    rerun: FidelityComparison,
    repeats: RepeatRow[],
    calls: number,
    tokens: number,
    channels: Channels[],
    langgraph?: LangGraphDetail,
  ): ScenarioResult {
    return new ScenarioResult(
      sc.name,
      sc.description,
      sc.topic,
      sc.steps.length,
      sc.fork_at_step,
      inheritedKeys(sc),
      tailKeys(sc),
      prefixOk,
      prefixMismatches,
      fork,
      rerun,
      repeats,
      calls,
      tokens,
      channels,
      langgraph,
    );
  }
}

function aggregateComparison(comparisons: FidelityComparison[]): FidelityComparison {
  const score = comparisons.reduce((a, c) => a + c.score, 0) / comparisons.length;
  const rationale = comparisons.filter((c) => c.rationale).map((c) => c.rationale).join(' | ');
  const samples = comparisons.flatMap((c) => c.samples);
  return { score, verdict: verdictFor(score), rationale, samples };
}

function isTransient(err: unknown): boolean {
  if (err instanceof ConnectionError) return true;
  if (err instanceof RateLimitError) return true;
  if (err instanceof APIError) return err.statusCode >= 500 || err.statusCode === 429;
  if (err instanceof SessionError) return true;
  return false;
}

function scenarioPassed(
  prefixOk: boolean,
  prefixMismatches: string[],
  fork: FidelityComparison,
  rerun: FidelityComparison,
  threshold: number,
  tolerance: number,
): { passed: boolean; failReason: string | null } {
  if (!prefixOk) {
    return { passed: false, failReason: `inherited prefix mismatch: ${prefixMismatches.join(', ')}` };
  }
  if (fork.score < threshold) {
    return { passed: false, failReason: `fork fidelity ${fork.score.toFixed(1)} < threshold ${threshold.toFixed(1)}` };
  }
  if (fork.score < rerun.score - tolerance) {
    return {
      passed: false,
      failReason:
        `fork fidelity ${fork.score.toFixed(1)} is > ${tolerance.toFixed(1)} below the no-fork ` +
        `re-run control (${rerun.score.toFixed(1)})`,
    };
  }
  return { passed: true, failReason: null };
}

interface RepeatRowData {
  prefixOk: boolean;
  prefixMismatches: string[];
  fork: FidelityComparison;
  rerun: FidelityComparison;
  calls: number;
  tokens: number;
  channels: Channels;
  observations: Observations;
  forkProvenance: ForkProvenance;
  langgraph?: LangGraphDetail;
}

async function runOneRepeat(
  actae: ActaeClient,
  sc: Scenario,
  opts: RunOpts & { judge: Judge },
): Promise<RepeatRowData> {
  const suffix = randomBytes(4).toString('hex');
  const baseCh = `fid-${sc.name}-${suffix}`;
  const rerunCh = `${baseCh}-rerun`;
  const forkCh = `${baseCh}-fork`;

  const base = await runPipeline(actae, sc, baseCh, opts);
  const rerun = await runPipeline(actae, sc, rerunCh, opts);
  const fork = await runFork(actae, sc, baseCh, forkCh, opts);

  const prefix = prefixIdentical(base.state, fork.state, sc);
  const forkCmp = await opts.judge.score(tailText(base.state, sc), tailText(fork.state, sc));
  const rerunCmp = await opts.judge.score(tailText(base.state, sc), tailText(rerun.state, sc));

  return {
    prefixOk: prefix.ok,
    prefixMismatches: prefix.mismatches,
    fork: forkCmp,
    rerun: rerunCmp,
    calls: base.registry.totalCalls() + rerun.registry.totalCalls() + fork.registry.totalCalls(),
    tokens: base.registry.totalTokens() + rerun.registry.totalTokens() + fork.registry.totalTokens(),
    channels: { baseline: baseCh, rerun: rerunCh, fork: forkCh },
    observations: {
      baseline: { state: base.state, trace: base.trace, tail: tailText(base.state, sc) },
      rerun: { state: rerun.state, trace: rerun.trace, tail: tailText(rerun.state, sc) },
      fork: { state: fork.state, trace: fork.trace, tail: tailText(fork.state, sc) },
    },
    forkProvenance: fork.provenance,
  };
}

interface SuiteOpts {
  adapter: Adapter;
  chat: DeepSeekClient | null;
  pipelineModel: string;
  judgeModel: string;
  dryRun: boolean;
  judgeReps: number;
  threshold: number;
  tolerance: number;
  repeats: number;
  repeatRetries: number;
}

class SuiteReport {
  readonly generatedAt: string;
  readonly adapter: Adapter;
  readonly pipelineModel: string;
  readonly judgeModel: string;
  readonly judgeTemperature: number;
  readonly judgeReps: number;
  readonly threshold: number;
  readonly tolerance: number;
  readonly dryRun: boolean;
  readonly scenarios: ScenarioResult[];
  readonly passed: boolean;
  readonly passedCount: number;
  readonly scenarioCount: number;
  readonly meanForkFidelity: number;
  readonly meanRerunFidelity: number;
  readonly worstForkFidelity: number;
  readonly totalLlmCalls: number;
  readonly totalTokens: number;

  constructor(
    generatedAt: string,
    adapter: Adapter,
    pipelineModel: string,
    judgeModel: string,
    judgeTemperature: number,
    judgeReps: number,
    threshold: number,
    tolerance: number,
    dryRun: boolean,
    scenarios: ScenarioResult[],
    passed: boolean,
    passedCount: number,
    scenarioCount: number,
    meanForkFidelity: number,
    meanRerunFidelity: number,
    worstForkFidelity: number,
    totalLlmCalls: number,
    totalTokens: number,
  ) {
    this.generatedAt = generatedAt;
    this.adapter = adapter;
    this.pipelineModel = pipelineModel;
    this.judgeModel = judgeModel;
    this.judgeTemperature = judgeTemperature;
    this.judgeReps = judgeReps;
    this.threshold = threshold;
    this.tolerance = tolerance;
    this.dryRun = dryRun;
    this.scenarios = scenarios;
    this.passed = passed;
    this.passedCount = passedCount;
    this.scenarioCount = scenarioCount;
    this.meanForkFidelity = meanForkFidelity;
    this.meanRerunFidelity = meanRerunFidelity;
    this.worstForkFidelity = worstForkFidelity;
    this.totalLlmCalls = totalLlmCalls;
    this.totalTokens = totalTokens;
  }

  toDict(): JsonObject {
    return {
      generated_at: this.generatedAt,
      adapter: this.adapter,
      pipeline_model: this.pipelineModel,
      judge_model: this.judgeModel,
      judge_temperature: this.judgeTemperature,
      judge_reps: this.judgeReps,
      threshold: this.threshold,
      tolerance: this.tolerance,
      dry_run: this.dryRun,
      scenarios: this.scenarios.map((s) => s.toDict()),
      overall: {
        pass: this.passed,
        passed: this.passedCount,
        scenarios: this.scenarioCount,
        mean_fork_fidelity: round2(this.meanForkFidelity),
        mean_rerun_fidelity: round2(this.meanRerunFidelity),
        worst_fork_fidelity: round2(this.worstForkFidelity),
        total_llm_calls: this.totalLlmCalls,
        total_tokens: this.totalTokens,
      },
    };
  }
}

function aggregate(results: ScenarioResult[], opts: SuiteOpts): SuiteReport {
  const passed = results.filter((r) => r.passed);
  const forkScores = results.map((r) => r.fork.score);
  const rerunScores = results.map((r) => r.rerun.score);
  const meanFork = forkScores.length ? forkScores.reduce((a, b) => a + b, 0) / forkScores.length : 0.0;
  const meanRerun = rerunScores.length ? rerunScores.reduce((a, b) => a + b, 0) / rerunScores.length : 0.0;
  const worstFork = forkScores.length ? Math.min(...forkScores) : 0.0;
  return new SuiteReport(
    nowUtc(),
    opts.adapter,
    opts.pipelineModel,
    opts.judgeModel,
    0.0,
    opts.judgeReps,
    opts.threshold,
    opts.tolerance,
    opts.dryRun,
    results,
    passed.length === results.length,
    passed.length,
    results.length,
    meanFork,
    meanRerun,
    worstFork,
    results.reduce((a, r) => a + r.calls, 0),
    results.reduce((a, r) => a + r.tokens, 0),
  );
}

// ---------------------------------------------------------------------------
// Full suite driver
// ---------------------------------------------------------------------------

async function repeatWithRetry(
  actae: ActaeClient,
  sc: Scenario,
  opts: RunOpts & { judge: Judge } & { adapter: Adapter },
  retries = 2,
): Promise<{ row: RepeatRowData; retried: boolean }> {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const row = opts.adapter === 'langgraph'
        ? await runOneLangGraphRepeat(actae, sc, opts)
        : await runOneRepeat(actae, sc, opts);
      return { row, retried: attempt > 0 };
    } catch (err) {
      if (!isTransient(err) || attempt === retries) throw err;
      await sleep(Math.min(10 * 2 ** attempt, 60) * 1000);
    }
  }
  throw new FidelityError('unreachable');
}

async function evaluateSuite(
  actae: ActaeClient,
  scenarios: Scenario[],
  opts: SuiteOpts,
): Promise<SuiteReport> {
  const judge: Judge = opts.dryRun ? new DeterministicJudge() : new LLMJudge(opts.chat!, { model: opts.judgeModel, reps: opts.judgeReps });

  const results: ScenarioResult[] = [];
  for (const sc of scenarios) {
    const forkCmps: FidelityComparison[] = [];
    const rerunCmps: FidelityComparison[] = [];
    const repeatRows: RepeatRow[] = [];
    let prefixOkAll = true;
    const prefixMismatchesAll: string[] = [];
    const channelsAll: Channels[] = [];
    let langgraphDetail: LangGraphDetail | undefined;
    let totalCalls = 0;
    let totalTokens = 0;

    for (let r = 0; r < opts.repeats; r++) {
      const { row, retried } = await repeatWithRetry(
        actae,
        sc,
        { chat: opts.chat, model: opts.pipelineModel, dryRun: opts.dryRun, judge, adapter: opts.adapter },
        opts.repeatRetries,
      );
      prefixOkAll = prefixOkAll && row.prefixOk;
      prefixMismatchesAll.push(...row.prefixMismatches);
      channelsAll.push(row.channels);
      forkCmps.push(row.fork);
      rerunCmps.push(row.rerun);
      if (row.langgraph) langgraphDetail = row.langgraph;
      repeatRows.push({
        fork_fidelity: round2(row.fork.score),
        rerun_fidelity: round2(row.rerun.score),
        prefix_identical: row.prefixOk,
        retried,
        channels: row.channels,
        observations: row.observations,
        fork_provenance: row.forkProvenance,
        ...(row.langgraph ? { langgraph: row.langgraph } : {}),
      });
      totalCalls += row.calls;
      totalTokens += row.tokens;
    }

    const forkAgg = aggregateComparison(forkCmps);
    const rerunAgg = aggregateComparison(rerunCmps);
    const { passed, failReason } = scenarioPassed(
      prefixOkAll,
      prefixMismatchesAll,
      forkAgg,
      rerunAgg,
      opts.threshold,
      opts.tolerance,
    );
    const result = ScenarioResult.fromParts(
      sc,
      prefixOkAll,
      prefixMismatchesAll,
      forkAgg,
      rerunAgg,
      repeatRows,
      totalCalls,
      totalTokens,
      channelsAll,
      langgraphDetail,
    );
    result.passed = passed;
    result.failReason = failReason;
    results.push(result);
  }

  return aggregate(results, opts);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function round2(x: number): number {
  return Math.round(x * 100) / 100;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolveFn) => setTimeout(resolveFn, ms));
}

function nowUtc(): string {
  const d = new Date();
  const p = (n: number) => String(n).padStart(2, '0');
  return (
    `${d.getUTCFullYear()}-${p(d.getUTCMonth() + 1)}-${p(d.getUTCDate())}T` +
    `${p(d.getUTCHours())}:${p(d.getUTCMinutes())}:${p(d.getUTCSeconds())}Z`
  );
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

interface Args {
  adapter: Adapter;
  dryRun: boolean;
  scenarios?: string;
  model: string;
  judgeModel: string;
  judgeReps: number;
  repeats: number;
  threshold: number;
  tolerance: number;
  reportDir: string;
  runId?: string;
}

function printHelp(): void {
  console.log(`Usage: node examples/fidelity/index.ts [options]

LLM-judged fork-resume fidelity suite (marketing-grade evidence).

Options:
  --adapter NAME         fork adapter: "session" (AgentSession.fork) or "langgraph"
                         (ActaeCheckpointSaver.forkThread on a real LangGraph StateGraph).
                         Default: session.
  --dry-run             deterministic fake pipeline + judge; no API key (plumbing only)
  --scenarios CSV       comma-separated scenario names (default: all)
  --model NAME          pipeline model (default: deepseek-chat)
  --judge-model NAME    judge model (default: deepseek-chat)
  --judge-reps N        judge samples averaged per comparison (default 2)
  --repeats N           independent scenario runs averaged per scenario (default 1; 3+ for marketing evidence)
  --threshold F         min fork fidelity to pass (default 7.0)
  --tolerance F         max gap fork below re-run to pass (default 1.5)
  --report-dir DIR      directory for JSON evidence reports (default: examples/fidelity-reports/, resolved relative to the repo root)
  --run-id ID           stable run id (default: timestamp)
  -h, --help            show this help`);
}

function parseArgs(argv: string[]): Args {
  const args: Args = {
    adapter: 'session',
    dryRun: false,
    model: process.env.DEEPSEEK_MODEL || 'deepseek-chat',
    judgeModel: process.env.DEEPSEEK_MODEL || 'deepseek-chat',
    judgeReps: 2,
    repeats: 1,
    threshold: 7.0,
    tolerance: 1.5,
    reportDir: 'examples/fidelity-reports',
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const next = (): string => {
      if (i + 1 >= argv.length) throw new FidelityError(`missing value for ${a}`);
      i++;
      return argv[i]!;
    };
    switch (a) {
      case '--adapter':
        args.adapter = next() as Adapter;
        if (args.adapter !== 'session' && args.adapter !== 'langgraph') {
          throw new FidelityError(`--adapter must be "session" or "langgraph", got ${args.adapter}`);
        }
        break;
      case '--dry-run':
        args.dryRun = true;
        break;
      case '--scenarios':
        args.scenarios = next();
        break;
      case '--model':
        args.model = next();
        break;
      case '--judge-model':
        args.judgeModel = next();
        break;
      case '--judge-reps':
        args.judgeReps = parseInt(next(), 10);
        break;
      case '--repeats':
        args.repeats = parseInt(next(), 10);
        break;
      case '--threshold':
        args.threshold = parseFloat(next());
        break;
      case '--tolerance':
        args.tolerance = parseFloat(next());
        break;
      case '--report-dir':
        args.reportDir = next();
        break;
      case '--run-id':
        args.runId = next();
        break;
      case '-h':
      case '--help':
        printHelp();
        process.exit(0);
        break;
      default:
        throw new FidelityError(`unknown argument: ${a}`);
    }
  }
  return args;
}

function scenariosByName(names: string): Scenario[] {
  const list = names.split(',').map((s) => s.trim()).filter(Boolean);
  const byName: Record<string, Scenario> = {};
  for (const sc of ALL_SCENARIOS) byName[sc.name] = sc;
  const unknown = list.filter((n) => !(n in byName));
  if (unknown.length) {
    throw new FidelityError(
      `unknown scenarios: ${unknown.join(', ')}; available: ${ALL_SCENARIOS.map((s) => s.name).join(', ')}`,
    );
  }
  return list.map((n) => byName[n]!);
}

function findRepoRoot(): string {
  // This file lives at <repo>/sdks/typescript/examples/fidelity/index.ts.
  const here = dirname(fileURLToPath(import.meta.url));
  return resolve(here, '..', '..', '..', '..');
}

function loadDeepseekKey(repoRoot: string): string {
  const envKey = process.env.DEEPSEEK_API_KEY;
  if (envKey) return envKey;
  const envPath = join(repoRoot, 'actae', '.env');
  if (existsSync(envPath)) {
    for (const line of readFileSync(envPath, 'utf-8').split(/\r?\n/)) {
      if (line.startsWith('DEEPSEEK_API_KEY=')) {
        return line.slice('DEEPSEEK_API_KEY='.length).trim().replace(/^["']|["']$/g, '');
      }
    }
  }
  throw new FidelityError(
    'DEEPSEEK_API_KEY not found. Set it in actae/.env or the environment ' +
      '(or run with --dry-run, which needs no API key).',
  );
}

function fmt(v: number, digits: number): string {
  return Number(v).toFixed(digits);
}

function summary(report: SuiteReport): string {
  const lines = [
    'Fidelity suite — LLM-judged fork vs no-fork re-run',
    `adapter: ${report.adapter}   generated_at: ${report.generatedAt}   ` +
      `pipeline_model: ${report.pipelineModel}   ` +
      `judge_model: ${report.judgeModel} (temp ${report.judgeTemperature}, reps ${report.judgeReps})`,
    `threshold: ${report.threshold}   tolerance: ${report.tolerance}   dry_run: ${report.dryRun ? 'True' : 'False'}   ` +
      `repeats: ${report.scenarios.length ? report.scenarios[0].repeats.length : 0}`,
    '',
  ];
  for (const r of report.scenarios) {
    const status = r.passed ? 'PASS' : 'FAIL';
    const repeatsTxt = r.repeats
      .map((row) => `fork=${fmt(row.fork_fidelity, 2)}/rerun=${fmt(row.rerun_fidelity, 2)}`)
      .join(', ');
    lines.push(
      `  [${status}] ${r.name.padEnd(22)} mean fork=${r.fork.score.toFixed(1).padStart(4)}  ` +
        `mean rerun=${r.rerun.score.toFixed(1).padStart(4)}  prefix_identical=${r.prefixIdentical ? 'True' : 'False'}`,
    );
    lines.push(`          per-repeat: ${repeatsTxt}`);
    if (r.failReason) lines.push(`          reason: ${r.failReason}`);
    lines.push(`          judge: ${r.fork.verdict} — ${r.fork.rationale.slice(0, 140)}`);
  }
  lines.push('');
  const o = report.toDict()['overall'] as JsonObject;
  lines.push(
    `  OVERALL: ${report.passed ? 'PASS' : 'FAIL'}  ${report.passedCount}/${report.scenarioCount} ` +
      `scenarios  mean fork=${o['mean_fork_fidelity']}  mean rerun=${o['mean_rerun_fidelity']}  ` +
      `worst fork=${o['worst_fork_fidelity']}  calls=${o['total_llm_calls']} tokens=${o['total_tokens']}`,
  );
  return lines.join('\n');
}

function writeReport(report: SuiteReport, reportDir: string, runId: string): string {
  mkdirSync(reportDir, { recursive: true });
  const json = JSON.stringify(report.toDict(), null, 2);
  const suffix = report.adapter === 'langgraph' ? '-langgraph-ts' : '-ts';
  const reportPath = join(reportDir, `fork-fidelity${suffix}-${runId}.json`);
  writeFileSync(reportPath, json, 'utf-8');
  writeFileSync(join(reportDir, `latest${suffix}.json`), json, 'utf-8');
  return reportPath;
}

// ---------------------------------------------------------------------------
// Scenarios — byte-for-byte ports of examples/fork_fidelity_scenarios.py
// ---------------------------------------------------------------------------

const S1_MARKET: Scenario = {
  name: 's1-market-entry',
  description: 'Market-entry analysis for a plant-based milk startup in Germany',
  topic: 'Launching a plant-based milk startup in Germany in 2026',
  fork_at_step: 2,
  params: { domain: 'market-analysis' },
  steps: [
    {
      key: 'research',
      system:
        'You are a market research analyst. Produce exactly 3 concrete, ' +
        'quantified findings about this market (size, growth, consumers).',
      user: (state) => state['topic'],
      temperature: 0.5,
      max_tokens: 350,
    },
    {
      key: 'competitors',
      system:
        'You are a competitive strategist. Name the 3 most relevant ' +
        'competitors and one positioning fact about each.',
      user: (state) => _s(state, ['research']),
      temperature: 0.5,
      max_tokens: 300,
    },
    {
      key: 'pricing',
      system:
        'You are a pricing strategist. Recommend a concrete pricing model ' +
        'with specific price points and margins.',
      user: (state) => _s(state, ['research', 'competitors']),
      temperature: 0.6,
      max_tokens: 350,
    },
    {
      key: 'risks',
      system: 'You are a risk analyst. List the top 4 risks with a one-line mitigation each.',
      user: (state) => _s(state, ['research', 'competitors', 'pricing']),
      temperature: 0.6,
      max_tokens: 350,
    },
    {
      key: 'recommendation',
      system:
        'You are the lead consultant. Write the final go/no-go recommendation ' +
        'with the 3 strongest reasons.',
      user: (state) => _s(state, ['research', 'competitors', 'pricing', 'risks']),
      temperature: 0.4,
      max_tokens: 350,
    },
  ],
};

const S2_CODE_SNIPPET = `def process_payment(order, user):
    total = sum(item["price"] * item["qty"] for item in order["items"])
    if total > user["balance"]:
        return "insufficient funds"
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO payments (user_id, total, ref) VALUES ('%s', '%s', '%s')" %
        (user["id"], total, order["ref"])
    )
    cursor.execute(
        "UPDATE users SET balance = balance - %s WHERE id = '%s'" %
        (total, user["id"])
    )
    conn.commit()
    return "ok"
`;

const S2_CODE_REVIEW: Scenario = {
  name: 's2-code-review',
  description: 'Security/correctness review of a payment-handling Python snippet',
  topic: 'Code review of a payment handler',
  input_text: S2_CODE_SNIPPET,
  fork_at_step: 3,
  params: { domain: 'code-review' },
  steps: [
    {
      key: 'understand',
      system: 'You are a senior engineer. Summarize in 3-4 sentences what this code does.',
      user: (state) => _s(state, ['_input']),
      temperature: 0.3,
      max_tokens: 250,
    },
    {
      key: 'bugs',
      system:
        'You are a code reviewer. List concrete bugs with severity and the ' +
        'line/section where each occurs.',
      user: (state) => _s(state, ['_input', 'understand']),
      temperature: 0.4,
      max_tokens: 400,
    },
    {
      key: 'security',
      system:
        'You are a security reviewer. List security issues: injection, ' +
        'authorization, data exposure. Be specific.',
      user: (state) => _s(state, ['_input']),
      temperature: 0.5,
      max_tokens: 350,
    },
    {
      key: 'improvements',
      system: 'You are a performance/quality expert. Suggest 3 concrete improvements.',
      user: (state) => _s(state, ['understand', 'bugs']),
      temperature: 0.5,
      max_tokens: 300,
    },
    {
      key: 'verdict',
      system:
        'You are the review lead. Give the final verdict: is this mergeable, ' +
        'and what are the top blocking issues.',
      user: (state) => _s(state, ['bugs', 'security', 'improvements']),
      temperature: 0.4,
      max_tokens: 350,
    },
  ],
};

const S3_SOURCE_TEXT =
  'Autoregressive large language models generate text token by token: at each ' +
  'step the model predicts a probability distribution over the vocabulary and ' +
  'samples the next token. Decoding strategies such as temperature scaling ' +
  'reshape that distribution to trade off diversity against determinism. KV ' +
  'caching avoids recomputing attention over earlier tokens, which is why ' +
  'longer contexts increase latency roughly linearly rather than quadratically.';

const S3_LOCALIZATION: Scenario = {
  name: 's3-localization',
  description: 'Translate an English technical paragraph to French and adapt it for a general audience',
  topic: 'Localization of an LLM technical explainer',
  input_text: S3_SOURCE_TEXT,
  fork_at_step: 1,
  params: { domain: 'localization' },
  steps: [
    {
      key: 'translate',
      system:
        'You are a professional technical translator. Translate the text ' +
        'to French, keeping the technical terms accurate.',
      user: (state) => _s(state, ['_input']),
      temperature: 0.3,
      max_tokens: 350,
    },
    {
      key: 'adapt',
      system:
        'You are an editor. Rewrite the French translation so a general ' +
        '(non-technical) audience can understand it, without changing the facts.',
      user: (state) => _s(state, ['translate']),
      temperature: 0.5,
      max_tokens: 350,
    },
    {
      key: 'glossary',
      system:
        'You are a terminology manager. List the 5 most important technical ' +
        'terms: English term, French translation, one-line explanation.',
      user: (state) => _s(state, ['_input', 'translate']),
      temperature: 0.4,
      max_tokens: 300,
    },
  ],
};

const S4_INCIDENT =
  'At 09:14 UTC the payments API started returning 5xx for ~12% of traffic. ' +
  'P95 latency rose from 120ms to 8s. At 09:22 a second deployment rolled out ' +
  'new auth middleware. At 09:31 the error rate hit 38% and the API was put in ' +
  'read-only mode. At 09:47 the auth middleware was rolled back; latency did ' +
  'NOT improve. At 10:02 the nightly batch job was restarted (it had crashed at ' +
  '09:08 and left 40,000 jobs queued, each holding a DB connection); latency ' +
  'recovered within minutes and the queue drained by 10:40. The DB connection ' +
  'pool maxed out at 09:26. Post-incident investigation confirmed the batch ' +
  "job's crash left queued jobs holding pooled connections until the pool " +
  'exhausted; the auth middleware rollout was coincidental and verified blameless.';

const S4_INCIDENT_ANALYSIS: Scenario = {
  name: 's4-incident-analysis',
  description: 'Postmortem of a fictional payments-API outage',
  topic: 'Outage postmortem analysis',
  input_text: S4_INCIDENT,
  fork_at_step: 1,
  params: { domain: 'incident-analysis' },
  steps: [
    {
      key: 'timeline',
      system:
        'You are an SRE. Reconstruct the incident timeline from the report, ' +
        'in order with timestamps and one-line evidence.',
      user: (state) => _s(state, ['_input']),
      temperature: 0.3,
      max_tokens: 300,
    },
    {
      key: 'rootcause',
      system:
        'You are the incident commander. Identify the most likely root cause ' +
        'and why it was not caught by monitoring.',
      user: (state) => _s(state, ['_input', 'timeline']),
      temperature: 0.5,
      max_tokens: 350,
    },
    {
      key: 'blastradius',
      system:
        'You are a systems analyst. Determine the blast radius (users, ' +
        'services, data integrity) and the customer impact.',
      user: (state) => _s(state, ['timeline', 'rootcause']),
      temperature: 0.5,
      max_tokens: 300,
    },
    {
      key: 'actions',
      system:
        'You are a reliability engineer. List concrete action items with ' +
        'owner and priority to prevent recurrence.',
      user: (state) => _s(state, ['timeline', 'rootcause', 'blastradius']),
      temperature: 0.4,
      max_tokens: 350,
    },
  ],
};

const S5_PRODUCT: Scenario = {
  name: 's5-product-brief',
  description: 'Product brief for adding offline mode to a note-taking app',
  topic: 'Adding an offline mode to a cross-platform note-taking app',
  fork_at_step: 3,
  params: { domain: 'product-brief' },
  steps: [
    {
      key: 'userresearch',
      system:
        'You are a product researcher. Synthesize 3 concrete user needs for ' +
        'offline mode, each with a supporting scenario.',
      user: (state) => state['topic'],
      temperature: 0.5,
      max_tokens: 350,
    },
    {
      key: 'spec',
      system:
        'You are a product manager. Write a concise functional spec: scope, ' +
        'MVP, non-goals, and 2 edge cases.',
      user: (state) => _s(state, ['userresearch']),
      temperature: 0.4,
      max_tokens: 400,
    },
    {
      key: 'gtm',
      system:
        'You are a growth lead. Outline the launch plan: target segments, ' +
        'messaging angle, and rollout.',
      user: (state) => _s(state, ['userresearch', 'spec']),
      temperature: 0.5,
      max_tokens: 300,
    },
    {
      key: 'metrics',
      system:
        'You are an analytics lead. Define 5 success metrics with baseline ' +
        'and target values.',
      user: (state) => _s(state, ['userresearch', 'spec', 'gtm']),
      temperature: 0.4,
      max_tokens: 300,
    },
  ],
};

const ALL_SCENARIOS: Scenario[] = [
  S1_MARKET,
  S2_CODE_REVIEW,
  S3_LOCALIZATION,
  S4_INCIDENT_ANALYSIS,
  S5_PRODUCT,
];

for (const sc of ALL_SCENARIOS) validateScenario(sc);

// ---------------------------------------------------------------------------
// Entrypoint
// ---------------------------------------------------------------------------

async function main(): Promise<number> {
  const args = parseArgs(process.argv.slice(2));
  if (args.judgeReps < 1 || args.repeats < 1) {
    throw new FidelityError('--judge-reps and --repeats must be >= 1');
  }
  const repoRoot = findRepoRoot();
  const scenarios = args.scenarios ? scenariosByName(args.scenarios) : ALL_SCENARIOS;

  let chat: DeepSeekClient | null = null;
  if (!args.dryRun) {
    chat = new DeepSeekClient(loadDeepseekKey(repoRoot));
  }

  const actae = newClientFromEnv({
    apiKey: process.env.ACTAE_API_KEY || 'sk-dev-0000000000000000000000',
    endpoint: process.env.ACTAE_URL || 'http://localhost:8002',
  });

  const report = await evaluateSuite(actae, scenarios, {
    adapter: args.adapter,
    chat,
    pipelineModel: args.model,
    judgeModel: args.judgeModel,
    dryRun: args.dryRun,
    judgeReps: args.judgeReps,
    threshold: args.threshold,
    tolerance: args.tolerance,
    repeats: args.repeats,
    repeatRetries: 2,
  });
  actae.disconnect();

  console.log(summary(report));
  const reportDir = resolve(repoRoot, args.reportDir);
  const runId = args.runId ?? report.generatedAt.replace(/:/g, '-').replace('T', '-').slice(0, 19);
  const reportPath = writeReport(report, reportDir, runId);
  console.log(`\n  report: ${reportPath}`);
  return report.passed ? 0 : 1;
}

main()
  .then((code) => process.exit(code))
  .catch((err) => {
    console.error('fidelity suite failed:', err instanceof Error ? err.message : String(err));
    process.exit(1);
  });
