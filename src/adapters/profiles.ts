import {
  FULL_FEATURE_SET,
  ActaeFeature,
  AdapterCapabilities,
} from './contract.js';

const durable = [
  ActaeFeature.Events,
  ActaeFeature.Replay,
  ActaeFeature.Checkpoints,
  ActaeFeature.Resume,
  ActaeFeature.Forks,
  ActaeFeature.CausalLineage,
] as const;
const observable = [
  ActaeFeature.Events,
  ActaeFeature.Replay,
  ActaeFeature.Checkpoints,
  ActaeFeature.Forks,
  ActaeFeature.CausalLineage,
] as const;
const reconstructed = [
  ActaeFeature.Checkpoints,
  ActaeFeature.Resume,
  ActaeFeature.Forks,
  ActaeFeature.CausalLineage,
] as const;

export const LANGGRAPH_CAPABILITIES = new AdapterCapabilities({
  framework: 'langgraph', adapterVersion: '2', supportLevel: 'certified',
  resumeFidelity: 'checkpoint_exact', features: durable, nativeCheckpoint: true,
  notes: 'Native checkpoint, pending-write, historical fork and resume integration.',
});
export const CLAUDE_CAPABILITIES = new AdapterCapabilities({
  framework: 'claude-agent-sdk', adapterVersion: '2', supportLevel: 'certified',
  resumeFidelity: 'session_native', features: durable, nativeCheckpoint: true,
  notes: 'SDK-native SessionStore transcripts, subkeys, summaries and forks.',
});
export const COPILOT_CAPABILITIES = new AdapterCapabilities({
  framework: 'github-copilot-sdk', adapterVersion: '2', supportLevel: 'certified',
  resumeFidelity: 'session_native', features: durable, nativeCheckpoint: true,
  notes: 'Native Copilot session events, durable state and session fork integration.',
});
export const LANGCHAIN_CAPABILITIES = new AdapterCapabilities({
  framework: 'langchain', adapterVersion: '2', supportLevel: 'preview',
  resumeFidelity: 'context_seeded', features: reconstructed, nativeCheckpoint: false,
  notes: 'Restores captured messages and tool outputs; runnable internals are not checkpointed.',
});
export const OPENAI_AGENTS_CAPABILITIES = new AdapterCapabilities({
  framework: 'openai-agents', adapterVersion: '3', supportLevel: 'preview',
  resumeFidelity: 'checkpoint_exact', features: durable, nativeCheckpoint: true,
  notes: 'Tracing plus native serializable RunState persistence, resume and Actae channel forks.',
});
export const CODEX_CAPABILITIES = new AdapterCapabilities({
  framework: 'codex', adapterVersion: '2', supportLevel: 'observability',
  resumeFidelity: 'observe_only', features: observable, nativeCheckpoint: false,
  notes: 'OTLP mirrors Codex runs; Codex rollout storage remains the native resume authority.',
});

export const ADAPTER_CAPABILITIES: ReadonlyMap<string, AdapterCapabilities> = new Map(
  [
    LANGGRAPH_CAPABILITIES,
    CLAUDE_CAPABILITIES,
    COPILOT_CAPABILITIES,
    LANGCHAIN_CAPABILITIES,
    OPENAI_AGENTS_CAPABILITIES,
    CODEX_CAPABILITIES,
  ].map((profile) => [profile.framework, profile]),
);

export function getAdapterCapabilities(framework: string): AdapterCapabilities {
  const profile = ADAPTER_CAPABILITIES.get(framework);
  if (!profile) {
    throw new RangeError(`unknown framework ${JSON.stringify(framework)}; available: ${[...ADAPTER_CAPABILITIES.keys()].sort().join(', ')}`);
  }
  return profile;
}

/** Complete Actae service surface beside a framework's native lifecycle adapter. */
export function commonSurfaceCapabilities(framework: string): AdapterCapabilities {
  const native = getAdapterCapabilities(framework);
  return new AdapterCapabilities({
    framework: native.framework,
    adapterVersion: native.adapterVersion,
    supportLevel: native.supportLevel,
    resumeFidelity: native.resumeFidelity,
    features: FULL_FEATURE_SET,
    nativeCheckpoint: native.nativeCheckpoint,
    notes: `${native.notes} Full Actae service surface is available through ActaeRunContext.`,
  });
}
