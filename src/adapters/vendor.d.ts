/**
 * Ambient declarations for optional framework modules used by the adapters.
 * The adapters load these modules with dynamic `import()` at call time only,
 * so the core SDK has no runtime dependency on them. These declarations let
 * the TypeScript compiler typecheck the dynamic imports even when the
 * packages are not installed.
 */
declare module 'openai-agents';
declare module '@openai/agents';
declare module '@openai/agents-core';
declare module '@anthropic-ai/claude-agent-sdk';
declare module 'claude-agent-sdk';
declare module '@github/copilot-sdk';
declare module '@langchain/langgraph';
declare module '@langchain/core';
