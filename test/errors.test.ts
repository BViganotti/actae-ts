import { describe, it, expect } from 'vitest';
import {
  ActaeError,
  APIError,
  AuthError,
  ChannelConflictError,
  ConnectionError,
  ConsumerError,
  ExecutionNotOwnedError,
  IdempotencyConflictError,
  IdempotencyKeyMismatchError,
  LockError,
  NoRestorableCheckpointError,
  NotFoundError,
  RateLimitError,
  ServerError,
  SessionCompletedError,
  SessionError,
  SnapshotBoundaryError,
  VersionConflictError,
  WakeupAlreadyFiredError,
} from '../src/errors.js';

describe('error hierarchy', () => {
  it('all errors extend ActaeError and Error', () => {
    const errors = [
      new AuthError('x'),
      new ConnectionError('x'),
      new RateLimitError(60, 'x'),
      new APIError(500, 'x'),
      new LockError('x'),
      new SnapshotBoundaryError('x'),
      new VersionConflictError('x'),
      new NoRestorableCheckpointError('x'),
      new IdempotencyConflictError('x'),
      new ChannelConflictError('x'),
      new ConsumerError('x'),
      new IdempotencyKeyMismatchError('x'),
      new ExecutionNotOwnedError('x'),
      new NotFoundError('x'),
      new ServerError('x'),
      new SessionError('x'),
      new SessionCompletedError('x'),
      new WakeupAlreadyFiredError('x'),
    ];
    for (const e of errors) {
      expect(e).toBeInstanceOf(ActaeError);
      expect(e).toBeInstanceOf(Error);
    }
  });

  it('instanceof works across subclass boundaries', () => {
    expect(new AuthError('x')).toBeInstanceOf(AuthError);
    expect(new AuthError('x')).not.toBeInstanceOf(ConnectionError);
    expect(new RateLimitError(10)).toBeInstanceOf(RateLimitError);
    expect(new APIError(500)).toBeInstanceOf(APIError);
  });

  it('RateLimitError carries retryAfterSeconds', () => {
    const e = new RateLimitError(60, 'slow down');
    expect(e.retryAfterSeconds).toBe(60);
    expect(e.message).toContain('60');
    expect(e.message).toContain('slow down');
  });

  it('APIError carries statusCode', () => {
    const e = new APIError(418, 'teapot');
    expect(e.statusCode).toBe(418);
    expect(e.message).toContain('418');
    expect(e.message).toContain('teapot');
  });

  it('error names are set for clean stack traces', () => {
    expect(new AuthError('x').name).toBe('AuthError');
    expect(new ConnectionError('x').name).toBe('ConnectionError');
  });

  it('NotFoundError / ServerError extend APIError', () => {
    expect(new NotFoundError()).toBeInstanceOf(APIError);
    expect(new NotFoundError().statusCode).toBe(404);
    expect(new ServerError()).toBeInstanceOf(APIError);
    expect(new ServerError().statusCode).toBe(500);
    expect(new ExecutionNotOwnedError()).not.toBeInstanceOf(NotFoundError);
  });
});
