import { parseJson, type JsonValue } from './json.js';

export interface BrowserFleetStreamOptions { url: string; ticket: string; signal?: AbortSignal; maxQueueFrames?: number }
/** Browser-safe, read-only fleet stream. The one-use ticket is sent in the
 * URL because browsers cannot set WebSocket Authorization headers.
 *
 * Deliberately ONE-SHOT (PR-019): a browser ticket is single-use, so there is
 * no transparent reconnect. On disconnect the generator ends; the caller
 * obtains a fresh ticket (via the customer backend / gateway) and starts a new
 * stream if desired. The internal queue is bounded by frames — a slow consumer
 * surfaces a typed lag error instead of unbounded buffering. Cancellation via
 * AbortSignal is propagated through connect and queue wait. */
export async function* fleetStream(opts: BrowserFleetStreamOptions): AsyncGenerator<JsonValue> {
  const url = new URL(opts.url); url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'; url.searchParams.set('ticket', opts.ticket);
  const socket = new WebSocket(url.toString()); const queue: JsonValue[]=[]; let wake:(()=>void)|undefined; let closed=false; let failure:Error|undefined;
  const cap = opts.maxQueueFrames ?? 2048;
  socket.onmessage=(event)=>{try{const item=parseJson(String(event.data)); if(queue.length>=cap){failure=new Error('browser fleet stream lag: queue overflow');socket.close();}else{queue.push(item);}}catch{/* malformed control frame */}wake?.();wake=undefined;}; socket.onerror=()=>{failure=new Error('fleet stream WebSocket error');wake?.();wake=undefined;}; socket.onclose=()=>{closed=true;wake?.();wake=undefined;};
  const abort=()=>{failure=new Error('aborted');socket.close();}; opts.signal?.addEventListener('abort',abort,{once:true});
  try { await new Promise<void>((resolve,reject)=>{socket.onopen=()=>resolve();socket.onerror=()=>reject(new Error('fleet stream connection failed'));}); for(;;){while(!queue.length&&!closed&&!failure)await new Promise<void>((resolve)=>{wake=resolve;});if(failure)throw failure;if(!queue.length)break;yield queue.shift()!;} } finally {opts.signal?.removeEventListener('abort',abort);socket.close();}
}
