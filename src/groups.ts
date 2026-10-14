import { randomUUID } from 'node:crypto';
import type { ActaeClient } from './client.js';
import type { GroupMessage, MemberLease } from './types.js';
import type { JsonObject, JsonValue } from './json.js';

const delay = (milliseconds: number) => new Promise<void>(resolve => setTimeout(resolve, milliseconds));

export class GroupSession {
  constructor(readonly client: ActaeClient, readonly groupId: string) {}
  member(memberId: string): GroupMemberSession { return new GroupMemberSession(this, memberId); }
  waitFor(member: string, event: string, opts: { after?: string; timeoutMs?: number; pollIntervalMs?: number; signal?: AbortSignal } = {}): Promise<GroupMessage> { return this.member(member).waitFor(event, opts); }
  promote(memberId: string): Promise<JsonObject> { return this.client.promoteExecutionGroupForkMember(this.groupId, memberId); }

  async waitAny(waits: Array<[string, string]>, opts: { timeoutMs?: number; pollIntervalMs?: number } = {}): Promise<[string, GroupMessage]> {
    if (waits.length === 0) throw new TypeError('waitAny requires at least one waiter');
    const controller = new AbortController();
    try {
      return await Promise.race(waits.map(async ([member, type]) => [member, await this.member(member).waitFor(type, { ...opts, signal: controller.signal })] as [string, GroupMessage]));
    } finally { controller.abort(); }
  }

  async waitAll(waits: Array<[string, string]>, opts: { timeoutMs?: number; pollIntervalMs?: number } = {}): Promise<Array<[string, GroupMessage]>> {
    return Promise.all(waits.map(async ([member, type]) => [member, await this.member(member).waitFor(type, opts)] as [string, GroupMessage]));
  }
}

export class GroupMemberSession {
  private leaseValue?: MemberLease;
  constructor(readonly group: GroupSession, readonly memberId: string) {}
  get lease(): MemberLease | undefined { return this.leaseValue; }

  async claim(ownerId = randomUUID(), leaseSeconds = 60): Promise<MemberLease> {
    this.leaseValue = await this.group.client.claimMember(this.group.groupId, this.memberId, ownerId, leaseSeconds);
    return this.leaseValue;
  }
  async heartbeat(leaseSeconds = 60): Promise<MemberLease> {
    if (!this.leaseValue) throw new Error('claim() must succeed before heartbeat()');
    this.leaseValue = await this.group.client.heartbeatMember(this.group.groupId, this.memberId, this.leaseValue.ownerId, this.leaseValue.generation, leaseSeconds);
    return this.leaseValue;
  }
  async release(): Promise<void> {
    if (!this.leaseValue) return;
    const lease = this.leaseValue; this.leaseValue = undefined;
    await this.group.client.releaseMember(this.group.groupId, this.memberId, lease.ownerId, lease.generation);
  }
  async send(to: string, type: string, payload: JsonValue, opts: { causalContext?: JsonObject; operationId?: string } = {}): Promise<GroupMessage> {
    return this.group.client.sendGroupMessage(this.group.groupId, this.memberId, to, type, payload, opts);
  }
  async messages(opts: { after?: string; limit?: number } = {}): Promise<GroupMessage[]> {
    return this.group.client.groupMessages(this.group.groupId, this.memberId, opts);
  }
  private async channelId(): Promise<string> {
    const members = await this.group.client.executionGroupMembers(this.group.groupId);
    const member = members.find(value => value.memberId === this.memberId);
    if (!member) throw new Error(`unknown execution-group member ${this.memberId}`);
    return member.channelId;
  }
  /** Subscribe this member's delivery channel on the live WebSocket. */
  async subscribeWebSocket(cursor?: number): Promise<void> {
    await this.group.client.subscribe(await this.channelId(), cursor, true);
  }
  /** Remove this member's delivery channel from the live WebSocket. */
  async unsubscribeWebSocket(): Promise<void> {
    await this.group.client.unsubscribe(await this.channelId());
  }
  /** Stream replayed and live deliveries from the member's WebSocket topic. */
  async *streamWebSocket(opts: { cursor?: number; signal?: AbortSignal } = {}): AsyncGenerator<GroupMessage> {
    const channel = await this.channelId();
    for await (const event of this.group.client.stream(channel, opts.cursor, opts.signal)) {
      if (event.eventType !== 'message.received') continue;
      const envelope = event.payload && typeof event.payload === 'object' && !Array.isArray(event.payload) ? event.payload as JsonObject : {};
      const from = envelope.from && typeof envelope.from === 'object' ? envelope.from as JsonObject : {};
      const to = envelope.to && typeof envelope.to === 'object' ? envelope.to as JsonObject : {};
      if (envelope.group_id !== this.group.groupId || to.member !== this.memberId) continue;
      yield {
        messageId: String(envelope.message_id ?? ''), groupId: String(envelope.group_id ?? ''),
        fromMemberId: String(from.member ?? ''), toMemberId: String(to.member ?? ''),
        messageType: String(envelope.type ?? ''), payload: (envelope.payload ?? null) as JsonValue,
        causalContext: envelope.causal_context as JsonObject | undefined, sourceEventId: '',
        deliveryEventId: event.id, status: 'delivered', acknowledgedAt: undefined,
      };
    }
  }
  async acknowledge(message: GroupMessage): Promise<GroupMessage> {
    if (!this.leaseValue) throw new Error('claim() must succeed before acknowledge()');
    return this.group.client.acknowledgeGroupMessage(message.messageId, this.leaseValue.ownerId, this.leaseValue.generation);
  }
  async waitFor(type: string, opts: { after?: string; timeoutMs?: number; pollIntervalMs?: number; signal?: AbortSignal } = {}): Promise<GroupMessage> {
    let after = opts.after; const started = Date.now(); const interval = opts.pollIntervalMs ?? 200;
    for (;;) {
      if (opts.signal?.aborted) throw new DOMException('wait cancelled', 'AbortError');
      for (const message of await this.messages({ after, limit: 100 })) {
        after = message.messageId;
        if (message.messageType === type) return message;
      }
      if (opts.timeoutMs !== undefined && Date.now() - started >= opts.timeoutMs) throw new Error(`timed out waiting for ${type}`);
      await delay(interval);
    }
  }
  async *stream(opts: { after?: string; pollIntervalMs?: number; signal?: AbortSignal } = {}): AsyncGenerator<GroupMessage> {
    let after = opts.after; const interval = opts.pollIntervalMs ?? 200;
    while (!opts.signal?.aborted) {
      const messages = await this.messages({ after, limit: 100 });
      if (messages.length === 0) { await delay(interval); continue; }
      for (const message of messages) { after = message.messageId; yield message; }
    }
  }
}
