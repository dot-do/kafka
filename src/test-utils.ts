/**
 * Test utilities for Kafdo
 */

/**
 * Creates a mock Durable Object ID
 */
export function createMockId(name: string): DurableObjectId {
  return {
    toString: () => `mock-id-${name}`,
    equals: (other: DurableObjectId) => other.toString() === `mock-id-${name}`,
    name,
  } as DurableObjectId
}

/**
 * Creates a test request with JSON body
 */
export function jsonRequest(
  url: string,
  method: string = 'POST',
  body?: unknown
): Request {
  return new Request(url, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  })
}

/**
 * Parses JSON response
 */
export async function parseResponse<T>(response: Response): Promise<T> {
  return response.json() as Promise<T>
}

/**
 * Test fixtures for common Kafdo entities
 */
export const fixtures = {
  topic: {
    name: 'test-topic',
    partitions: 4,
    replicationFactor: 1,
  },

  message: {
    key: 'test-key',
    value: { data: 'test-value' },
    headers: { 'x-test': 'header' },
  },

  consumerGroup: {
    groupId: 'test-group',
    clientId: 'test-client',
  },
}
