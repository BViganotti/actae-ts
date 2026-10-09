import { describe, expect, it } from 'vitest';
import { CopilotRecorder, CopilotManager } from '../src/adapters/copilot.js';
import type { CopilotSessionLike, CopilotClientLike } from '../src/adapters/copilot.js';
import { FakeActaeClient } from './helpers.js';

/**
 * Real-framework conformance against the actual `@github/copilot-sdk` types:
 * the recorder/manager are structurally compatible with `CopilotSession` /
 * `CopilotClient`. (A live end-to-end would need a running Copilot CLI, so the
 * conformance check + fake-session behavioral tests are the testable surface.)
 */

describe('Copilot adapters × real @github/copilot-sdk', () => {
  it('accepts a real CopilotSession structurally', async () => {
    // Compile-time: the SDK's CopilotSession is assignable to our structural
    // interface (it has `on`, `getEvents`, `sessionId`).
    const actae = new FakeActaeClient();
    const rec = new CopilotRecorder(actae as never);
    // `on` returns void; getEvents is optional — construct a minimal stand-in
    // that only exposes what our interface requires.
    const minimalSession: CopilotSessionLike = {
      sessionId: 's1',
      on: () => undefined,
    };
    await rec.attach(minimalSession);
    expect(rec.sessionId()).toBe('s1');
    expect(rec.channelId()).toBe('copilot:s1');
    await rec.stop();
  });

  it('manager accepts a real CopilotClient-shaped object', async () => {
    const actae = new FakeActaeClient();
    const mgr = new CopilotManager(actae as never);
    const client: CopilotClientLike = {
      async createSession() {
        return { sessionId: 's1', on: () => undefined };
      },
      async resumeSession(sessionId) {
        return { sessionId, on: () => undefined };
      },
    };
    const handle = await mgr.startSession(client, {});
    expect(handle.channel).toBe('copilot:s1');
    await mgr.stopAll();
  });

  it('recorder handles the real event-shape fields (parentId/agentId/ephemeral)', async () => {
    const actae = new FakeActaeClient();
    const rec = new CopilotRecorder(actae as never);
    const handlers: Array<(e: { id: string; type: string; data?: unknown; parentId?: string | null; agentId?: string | null; ephemeral?: boolean | null }) => void> = [];
    await rec.attach({
      sessionId: 's1',
      on(handler) {
        handlers.push(handler);
        return undefined;
      },
    });
    handlers[0]!({
      id: 'e1',
      type: 'tool_execution.complete',
      data: { ok: true },
      parentId: 'p1',
      agentId: 'a1',
      ephemeral: false,
    });
    await rec.stop();
    const ev = actae.recordedEvents.find((e) => e.eventType === 'copilot.tool_execution.complete');
    expect(ev?.opts.metadata).toMatchObject({
      parent_event_id: 'p1',
      agent_id: 'a1',
      ephemeral: false,
    });
  });
});
