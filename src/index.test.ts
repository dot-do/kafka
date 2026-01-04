import { describe, it, expect } from 'vitest'
import { SELF, env } from 'cloudflare:test'

describe('Kafdo Worker', () => {
  it('responds with status info on root', async () => {
    const response = await SELF.fetch('http://localhost/')
    expect(response.status).toBe(200)

    const data = await response.json() as { name: string; status: string }
    expect(data.name).toBe('kafdo')
    expect(data.status).toBe('ok')
  })

  it('responds to health check', async () => {
    const response = await SELF.fetch('http://localhost/health')
    expect(response.status).toBe(200)

    const data = await response.json() as { status: string }
    expect(data.status).toBe('healthy')
  })

  it('has Durable Object bindings available', () => {
    expect(env.TOPIC_PARTITION).toBeDefined()
    expect(env.CONSUMER_GROUP).toBeDefined()
    expect(env.CLUSTER_METADATA).toBeDefined()
  })

  it('can get Durable Object stub', () => {
    const id = env.TOPIC_PARTITION.idFromName('test-topic-0')
    const stub = env.TOPIC_PARTITION.get(id)
    expect(stub).toBeDefined()
  })
})
