import { describe, expect, it } from 'vitest';
import { GroupSession } from '../src/groups.js';

const makeMessage = (id: string, member: string, type: string) => ({ messageId:id, groupId:'g', fromMemberId:'router', toMemberId:member, messageType:type, payload:{}, sourceEventId:'s', deliveryEventId:'d', status:'delivered', createdAt:new Date() });

describe('GroupSession', () => {
  it('replays durable waits, releases leases, and promotes frozen members', async () => {
    const queues: Record<string, any[][]> = { a:[[makeMessage('1','a','done')]], b:[[],[makeMessage('2','b','candidate')]] };
    let released=false;
    const client:any={
      groupMessages:async (_g:string,m:string)=>queues[m]!.shift()??[],
      claimMember:async (g:string,m:string,o:string)=>({groupId:g,memberId:m,ownerId:o,generation:2,leaseUntil:new Date()}),
      releaseMember:async()=>{released=true}, heartbeatMember:async()=>{throw new Error('unexpected')},
      promoteExecutionGroupForkMember:async(g:string,m:string)=>({fork_group_id:g,member_policies:{[m]:'reactive'}}),
    };
    const group=new GroupSession(client,'g');
    expect((await group.waitFor('a','done')).messageId).toBe('1');
    expect((await group.waitAny([['b','candidate']],{timeoutMs:1000,pollIntervalMs:1}))[0]).toBe('b');
    const member=group.member('a'); await member.claim('worker'); await member.release(); expect(released).toBe(true);
    expect((await group.promote('a'))['member_policies']).toEqual({a:'reactive'});
  });
  it('streams only matching group deliveries over WebSocket', async () => {
    const client:any = {
      executionGroupMembers: async () => [{ groupId:'g', memberId:'a', channelId:'member-a', generation:0, metadata:{} }],
      stream: async function* (topic:string) {
        expect(topic).toBe('member-a');
        yield { id:'noise', channelId:topic, eventType:'broadcast', payload:{}, actor:'', cursor:1, timestamp:'' };
        yield { id:'delivery', channelId:topic, eventType:'message.received', cursor:2, timestamp:'', actor:'', payload:{
          message_id:'m1', group_id:'g', from:{member:'router'}, to:{member:'a'}, type:'task.done', payload:{ok:true},
        } };
      },
    };
    const values=[]; for await (const value of new GroupSession(client,'g').member('a').streamWebSocket()) values.push(value);
    expect(values).toHaveLength(1); expect(values[0]!.messageId).toBe('m1'); expect(values[0]!.deliveryEventId).toBe('delivery');
  });
});
