/**
 * LangChain.js State Adapter for Actae.
 *
 * Mirrors the Python adapter (`actae_client.adapters.langchain`):
 * `ActaeContextSaver` is a callback handler that automatically saves agent
 * context (messages, tool outputs, chain metadata) as cursor-aligned state
 * snapshots, and `ChainResumer` restarts a chain with full context recovery.
 *
 * ```ts
 * import { ActaeContextSaver, ChainResumer } from '@actae/sdk/adapters/langchain';
 * const saver = new ActaeContextSaver(actae, { channel: 'my-chain' });
 * await chain.invoke(input, { callbacks: [saver] });
 *
 * // On restart:
 * const resumer = new ChainResumer(actae, { channel: 'my-chain' });
 * const response = await resumer.resume(chain, defaultInput);
 * ```
 *
 * The callback interface is structural (no runtime dependency on
 * `@langchain/langchain`): LangChain.js invokes whichever handler methods
 * exist on the object.
 */

import type { ActaeClient } from '../client.js';
import { adapterCheckpointState } from './contract.js';
import type { JsonObject, JsonValue } from '../json.js';

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

function serialize(value: unknown): JsonValue {
  if (value === null || value === undefined) return null;
  const type = typeof value;
  if (type === 'string' || type === 'number' || type === 'boolean' || type === 'bigint') {
    return value as JsonValue;
  }
  if (Array.isArray(value)) return value.map(serialize);
  if (type === 'object') {
    const out: JsonObject = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      try {
        out[k] = serialize(v);
      } catch {
        out[k] = String(v);
      }
    }
    return out;
  }
  return String(value);
}

function stringify(value: unknown): string {
  try {
    if (value === null || value === undefined) return '';
    if (typeof value === 'string') return value;
    return JSON.stringify(serialize(value));
  } catch {
    return String(value);
  }
}

// ---------------------------------------------------------------------------
// ActaeContextSaver
// ---------------------------------------------------------------------------

export interface ActaeContextSaverOptions {
  /** Channel ID for context storage (default "langchain"). */
  channel?: string;
  /** Save context snapshot every N callbacks (default 3). */
  saveEveryN?: number;
}

/** LangChain.js callback handler that saves full agent context as
 * cursor-aligned state snapshots. Captures messages, tool outputs, chain
 * metadata, and LLM responses at each step so the agent can resume with full
 * context. */
export class ActaeContextSaver {
  readonly actae: ActaeClient;
  readonly channel: string;
  readonly saveEveryN: number;
  readonly name = 'ActaeContextSaver';

  private context: JsonObject = {
    messages: [] as JsonValue[],
    tool_outputs: [] as JsonValue[],
    chain_steps: [] as JsonValue[],
  };
  private stepCount = 0;
  private writes: Array<Promise<void>> = [];

  constructor(actae: ActaeClient, options: ActaeContextSaverOptions = {}) {
    this.actae = actae;
    this.channel = options.channel ?? 'langchain';
    this.saveEveryN = options.saveEveryN ?? 3;
  }

  async handleLLMEnd(output: unknown, runId?: string): Promise<void> {
    this.stepCount++;
    let responseText = '';
    try {
      const generations = (output as { generations?: unknown })?.generations;
      if (Array.isArray(generations)) {
        for (const genList of generations) {
          if (Array.isArray(genList)) {
            for (const gen of genList) {
              const g = gen as { text?: string; message?: unknown };
              if (typeof g.text === 'string' && g.text !== '') responseText += g.text;
              else if (g.message !== undefined) responseText += stringify(g.message);
            }
          }
        }
      }
    } catch {
      responseText = stringify(output).slice(0, 500);
    }
    if (responseText === '') responseText = stringify(output).slice(0, 500);

    const messages = this.context['messages'];
    if (Array.isArray(messages)) {
      messages.push({
        role: 'assistant',
        content: responseText,
        run_id: runId ?? null,
      });
    }
    this.maybeSave();
  }

  async handleToolEnd(output: unknown, runId?: string): Promise<void> {
    this.stepCount++;
    const toolOutputs = this.context['tool_outputs'];
    if (Array.isArray(toolOutputs)) {
      toolOutputs.push({ output: serialize(output), run_id: runId ?? null });
    }
    this.maybeSave();
  }

  async handleChainEnd(outputs: unknown, runId?: string): Promise<void> {
    const chainSteps = this.context['chain_steps'];
    if (Array.isArray(chainSteps)) {
      chainSteps.push({ outputs: serialize(outputs), run_id: runId ?? null });
    }
    await this.saveNow();
  }

  /** Returns the latest saved context, or undefined when nothing has been
   * saved yet. */
  async loadContext(): Promise<JsonObject | undefined> {
    await this.drain();
    const snapshot = await this.actae.latestState(this.channel);
    const ctx = snapshot?.state?.['context'];
    return typeof ctx === 'object' && ctx !== null && !Array.isArray(ctx)
      ? (ctx as JsonObject)
      : undefined;
  }

  /** Waits for any in-flight background saves to complete. */
  async flush(): Promise<void> {
    await this.drain();
  }

  private maybeSave(): void {
    if (this.stepCount % this.saveEveryN === 0) void this.saveNow();
  }

  /** Fire-and-forget save (does not block the chain). */
  private saveNow(): Promise<void> {
    const p = this.doSave().catch((err: Error) => {
      // Context saver must never break the chain.
      console.warn(`actae: ActaeContextSaver save failed: ${err.message}`);
    });
    this.writes.push(p);
    p.finally(() => {
      const idx = this.writes.indexOf(p);
      if (idx >= 0) this.writes.splice(idx, 1);
    });
    return p;
  }

  private async doSave(): Promise<void> {
    const cursor = (await this.actae.latestCursor(this.channel)) ?? 0;
    await this.actae.saveState(this.channel, cursor, adapterCheckpointState(
      { context: this.context },
      {
        framework: 'langchain',
        channelId: this.channel,
        portableState: {
          message_count: Array.isArray(this.context['messages']) ? this.context['messages'].length : 0,
          tool_output_count: Array.isArray(this.context['tool_outputs']) ? this.context['tool_outputs'].length : 0,
          chain_step_count: Array.isArray(this.context['chain_steps']) ? this.context['chain_steps'].length : 0,
        },
        nativeCheckpoint: { resume: 'context_seeded' },
        eventCursor: cursor,
      },
    ));
  }

  private async drain(): Promise<void> {
    while (this.writes.length > 0) {
      const pending = [...this.writes];
      await Promise.all(pending);
    }
  }
}

// ---------------------------------------------------------------------------
// ChainResumer
// ---------------------------------------------------------------------------

/** Resumes a LangChain.js chain from the last Actae state snapshot. */
export class ChainResumer {
  readonly actae: ActaeClient;
  readonly channel: string;
  private readonly saver: ActaeContextSaver;

  constructor(actae: ActaeClient, options: ActaeContextSaverOptions = {}) {
    this.actae = actae;
    this.channel = options.channel ?? 'langchain';
    this.saver = new ActaeContextSaver(actae, { channel: this.channel });
  }

  /** Runs the chain, seeded from the last saved context if available. If a
   * previous context exists, the last assistant message is used as input;
   * otherwise `defaultInput` is used. */
  async resume(chain: { invoke: (input: unknown, config?: unknown) => Promise<unknown> | unknown }, defaultInput?: unknown): Promise<unknown> {
    const context = await this.saver.loadContext();
    if (!context || !Array.isArray(context['messages']) || context['messages'].length === 0) {
      return chain.invoke(defaultInput, { callbacks: [this.saver] });
    }
    const messages = context['messages'] as JsonValue[];
    const last = messages[messages.length - 1] as JsonObject;
    const lastMsg = typeof last?.['content'] === 'string' ? last['content'] : undefined;
    return chain.invoke(lastMsg ?? defaultInput, { callbacks: [this.saver] });
  }

  /** Forks this chain's context into a new channel and returns a
   * ChainResumer bound to it — the LangChain-native counterpart of
   * StateManager.fork(). */
  async fork(newChannel: string, options: { reason?: string } = {}): Promise<ChainResumer> {
    const cursor = (await this.actae.latestCursor(this.channel)) ?? 0;
    try {
      await this.actae.fork(this.channel, newChannel, cursor, {
        displayName: newChannel,
        reason: options.reason ?? `Forked ${this.channel} → ${newChannel}`,
      });
    } catch {
      await this.actae.fork(this.channel, newChannel, 0, {
        displayName: newChannel,
        reason: options.reason ?? `Forked ${this.channel} → ${newChannel}`,
      });
    }
    return new ChainResumer(this.actae, { channel: newChannel });
  }
}
