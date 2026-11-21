import assert from 'node:assert/strict';
import net from 'node:net';
import { randomInt } from 'node:crypto';

const PORT_RANGE_START = 20000;
const PORT_RANGE_LEN = 12000; // 20000..31999, below Linux/macOS ephemeral ranges.

function parseDistinctPorts(raw, count, envName) {
  const tokens = raw.trim().split(/\s+/).filter(Boolean);
  assert.equal(
    tokens.length,
    count,
    `${envName} must specify exactly ${count} whitespace-separated ports`,
  );
  const ports = tokens.map((token) => {
    assert.match(token, /^\d+$/, `${envName} contains invalid port "${token}"`);
    const port = Number(token);
    assert.ok(port >= 1 && port <= 65535, `${envName} port out of range: ${token}`);
    return port;
  });
  assert.equal(new Set(ports).size, count, `${envName} must not contain duplicate ports`);
  return ports;
}

function tryListen(port) {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once('error', reject);
    server.listen({ host: '127.0.0.1', port, exclusive: true }, () => {
      resolve(server);
    });
  });
}

export async function allocateDistinctPorts(count, envName) {
  const preallocated = envName ? process.env[envName] : undefined;
  if (preallocated && preallocated.trim().length > 0) {
    return parseDistinctPorts(preallocated, count, envName);
  }

  const servers = [];
  const ports = [];
  const offset = randomInt(PORT_RANGE_LEN);
  try {
    for (let step = 0; step < PORT_RANGE_LEN && servers.length < count; step += 1) {
      const candidate = PORT_RANGE_START + ((offset + step) % PORT_RANGE_LEN);
      try {
        const server = await tryListen(candidate);
        servers.push(server);
        ports.push(server.address().port);
      } catch {}
    }
    while (servers.length < count) {
      const server = await tryListen(0);
      servers.push(server);
      ports.push(server.address().port);
    }
    return ports;
  } finally {
    await Promise.all(
      servers.map((server) => new Promise((resolve) => server.close(() => resolve()))),
    );
  }
}
