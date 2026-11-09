import http from 'node:http';
import { readFile, mkdir, writeFile, rename } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { randomUUID, randomBytes, createHash } from 'node:crypto';
import WebSocket, { WebSocketServer } from 'ws';

const __dirname = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT || 3000);
const DATA_DIR = process.env.BLOCKYARD_DATA_DIR || join(__dirname, 'data');
const DATA_FILE = join(DATA_DIR, 'worlds.json');
const MAX_PLAYERS_PER_WORLD = 24;
const MAX_BLOCKS_PER_WORLD = 300;
const MAX_WORLD_TITLE = 60;
const MAX_MESSAGE_BYTES = 8 * 1024;
const worlds = new Map();
let saveQueue = Promise.resolve();

function cleanName(value) {
  const cleaned = String(value ?? 'Builder').replace(/[\u0000-\u001f\u007f<>]/g, '').trim().slice(0, 20);
  return cleaned || 'Builder';
}
function cleanTitle(value) {
  const cleaned = String(value ?? '').replace(/[\u0000-\u001f\u007f<>]/g, '').replace(/\s+/g, ' ').trim().slice(0, MAX_WORLD_TITLE);
  return cleaned || 'Untitled experience';
}
function cleanWorldId(value) {
  const id = String(value ?? '').trim().slice(0, 64);
  return /^[a-zA-Z0-9_-]{1,64}$/.test(id) ? id : null;
}
function finiteNumber(value, min, max) {
  return typeof value === 'number' && Number.isFinite(value) && value >= min && value <= max;
}
function tokenHash(token) {
  return createHash('sha256').update(String(token || '')).digest('hex');
}
function safeSend(ws, packet) {
  if (ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(packet)); } catch { /* Socket may close during send. */ }
  }
}
function sendError(ws, message) { safeSend(ws, { type: 'error', message }); }
function broadcast(world, packet, except = null) {
  for (const client of world.clients.values()) if (client.ws !== except) safeSend(client.ws, packet);
}
function onlinePlayerCount() {
  let count = 0;
  for (const world of worlds.values()) count += world.clients.size;
  return count;
}
function publicWorldRecord(world) {
  return {
    id: world.id,
    title: world.title,
    ownerName: world.ownerName || 'Blockyard Creator',
    createdAt: world.createdAt,
    updatedAt: world.updatedAt,
    playerCount: world.clients.size,
    blockCount: world.blocks.size,
  };
}
function persistentWorldRecord(world) {
  return {
    id: world.id,
    title: world.title,
    ownerName: world.ownerName || 'Blockyard Creator',
    createdAt: world.createdAt,
    updatedAt: world.updatedAt,
    published: true,
    ownerTokenHash: world.ownerTokenHash || null,
    blocks: [...world.blocks.values()],
  };
}
function createWorld({ id, title, ownerName, published = false, ownerTokenHash = null, blocks = [] }) {
  const now = new Date().toISOString();
  return {
    id,
    title: cleanTitle(title),
    ownerName: cleanName(ownerName || 'Builder'),
    createdAt: now,
    updatedAt: now,
    published,
    ownerTokenHash,
    clients: new Map(),
    blocks: new Map(blocks.filter(b => b && typeof b.id === 'string').map(b => [b.id, b])),
  };
}

async function loadWorlds() {
  await mkdir(DATA_DIR, { recursive: true });
  let saved = null;
  try {
    saved = JSON.parse(await readFile(DATA_FILE, 'utf8'));
  } catch (err) {
    if (err.code !== 'ENOENT') console.warn('Could not read saved worlds; starting with the default world:', err.message);
  }
  if (saved && Array.isArray(saved.worlds)) {
    for (const item of saved.worlds) {
      const id = cleanWorldId(item?.id);
      if (!id || !item.published) continue;
      const validBlocks = Array.isArray(item.blocks) ? item.blocks.filter(b =>
        b && typeof b.id === 'string' && /^[a-zA-Z0-9_-]{1,100}$/.test(b.id) &&
        finiteNumber(b.x, -22, 22) && finiteNumber(b.y, 1, 101) && finiteNumber(b.z, -22, 22) &&
        finiteNumber(b.yaw, -1000, 1000) && Number.isInteger(b.color) && b.color >= 0 && b.color <= 0xffffff
      ).slice(0, MAX_BLOCKS_PER_WORLD) : [];
      const world = createWorld({
        id,
        title: item.title,
        ownerName: item.ownerName,
        published: true,
        ownerTokenHash: typeof item.ownerTokenHash === 'string' ? item.ownerTokenHash : null,
        blocks: validBlocks,
      });
      world.createdAt = item.createdAt || world.createdAt;
      world.updatedAt = item.updatedAt || world.updatedAt;
      worlds.set(id, world);
    }
  }
  if (!worlds.has('baseplate')) {
    const base = createWorld({ id: 'baseplate', title: 'Small Property Baseplate', ownerName: 'Blockyard', published: true });
    worlds.set(base.id, base);
  }
  await persistWorlds();
}

function persistWorlds() {
  const snapshot = JSON.stringify({ version: 1, worlds: [...worlds.values()].filter(w => w.published).map(persistentWorldRecord) }, null, 2);
  saveQueue = saveQueue.catch(() => {}).then(async () => {
    await mkdir(DATA_DIR, { recursive: true });
    const temp = `${DATA_FILE}.tmp`;
    await writeFile(temp, snapshot, 'utf8');
    await rename(temp, DATA_FILE);
  });
  return saveQueue;
}

function catalogPayload() {
  const experiences = [...worlds.values()]
    .filter(world => world.published)
    .sort((a, b) => String(b.updatedAt).localeCompare(String(a.updatedAt)))
    .map(publicWorldRecord);
  return { experiences, onlinePlayers: onlinePlayerCount(), publishedWorlds: experiences.length };
}
function broadcastCatalogChanged() {
  const packet = { type: 'catalog:changed', onlinePlayers: onlinePlayerCount() };
  for (const ws of wss.clients) safeSend(ws, packet);
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const pathname = url.pathname;
  if (pathname === '/healthz') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    res.end(JSON.stringify({ ok: true, worlds: [...worlds.values()].filter(w => w.published).length, players: onlinePlayerCount() }));
    return;
  }
  if (pathname === '/api/experiences') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store', 'access-control-allow-origin': 'same-origin' });
    res.end(JSON.stringify(catalogPayload()));
    return;
  }
  if (pathname === '/api/stats') {
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    res.end(JSON.stringify({ onlinePlayers: onlinePlayerCount(), publishedWorlds: [...worlds.values()].filter(w => w.published).length }));
    return;
  }
  const experienceMatch = pathname.match(/^\/api\/experiences\/([a-zA-Z0-9_-]{1,64})$/);
  if (experienceMatch) {
    const world = worlds.get(experienceMatch[1]);
    if (!world?.published) {
      res.writeHead(404, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
      res.end(JSON.stringify({ error: 'Experience not found.' }));
      return;
    }
    res.writeHead(200, { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' });
    res.end(JSON.stringify(publicWorldRecord(world)));
    return;
  }
  if (pathname === '/favicon.ico') { res.writeHead(204); res.end(); return; }
  if (pathname === '/' || pathname === '/index.html' || pathname === '/play' || pathname === '/play.html') {
    try {
      const html = await readFile(join(__dirname, 'index.html'));
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
      res.end(html);
    } catch {
      res.writeHead(500, { 'content-type': 'text/plain; charset=utf-8' });
      res.end('Could not load index.html');
    }
    return;
  }
  res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Not found');
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: MAX_MESSAGE_BYTES, perMessageDeflate: false });

wss.on('connection', ws => {
  ws.isAlive = true;
  ws.world = null;
  ws.client = null;
  ws.canEdit = false;
  ws.lastStateAt = 0;
  ws.lastChatAt = 0;
  ws.editWindowAt = Date.now();
  ws.editCount = 0;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('error', err => console.warn('WebSocket error:', err.message));

  ws.on('message', async raw => {
    let packet;
    try { packet = JSON.parse(raw.toString()); }
    catch { sendError(ws, 'Message must be valid JSON.'); return; }
    if (!packet || typeof packet.type !== 'string') { sendError(ws, 'Message type is missing.'); return; }

    if (!ws.client) {
      if (packet.type !== 'join') { sendError(ws, 'Send a join message first.'); return; }
      const worldId = cleanWorldId(packet.worldId || packet.room || 'baseplate');
      if (!worldId) { sendError(ws, 'Invalid experience ID.'); return; }
      let world = worlds.get(worldId);
      let newOwnerToken = null;
      if (!world && packet.create === true) {
        newOwnerToken = randomBytes(32).toString('hex');
        world = createWorld({
          id: worldId,
          title: packet.title || 'Untitled experience',
          ownerName: cleanName(packet.name),
          published: false,
          ownerTokenHash: tokenHash(newOwnerToken),
        });
        worlds.set(worldId, world);
      }
      if (!world) { sendError(ws, 'That experience does not exist. Refresh Discover and try again.'); return; }
      if (world.clients.size >= MAX_PLAYERS_PER_WORLD) {
        sendError(ws, `This experience is full (${MAX_PLAYERS_PER_WORLD} players maximum).`);
        ws.close(1008, 'Experience full');
        return;
      }
      const client = {
        id: randomUUID(),
        name: cleanName(packet.name),
        color: ['#d85a48', '#9b68d8', '#e0a52f', '#35a6a0', '#d56ba4', '#6489db'][world.clients.size % 6],
        state: { x: 0, y: 0.12, z: 0, yaw: 0 },
        ws,
      };
      ws.world = world;
      ws.client = client;
      ws.canEdit = !!newOwnerToken || !world.ownerTokenHash || (!!packet.ownerToken && tokenHash(packet.ownerToken) === world.ownerTokenHash);
      world.clients.set(client.id, client);
      safeSend(ws, {
        type: 'welcome',
        id: client.id,
        name: client.name,
        room: world.id,
        worldId: world.id,
        title: world.title,
        published: world.published,
        canEdit: ws.canEdit,
        ownerToken: newOwnerToken,
        players: [...world.clients.values()].filter(p => p.id !== client.id).map(p => ({ id: p.id, name: p.name, color: p.color, ...p.state })),
        blocks: [...world.blocks.values()],
      });
      broadcast(world, { type: 'player:joined', player: { id: client.id, name: client.name, color: client.color, ...client.state } }, ws);
      broadcast(world, { type: 'room:count', count: world.clients.size });
      broadcastCatalogChanged();
      console.log(`${client.name} joined ${world.id} (${world.clients.size}/${MAX_PLAYERS_PER_WORLD})`);
      return;
    }

    const world = ws.world;
    const client = ws.client;
    if (!world || !client) return;

    if (packet.type === 'state') {
      const now = Date.now();
      if (now - ws.lastStateAt < 25) return;
      ws.lastStateAt = now;
      if (!finiteNumber(packet.x, -22.5, 22.5) || !finiteNumber(packet.y, 0, 110) ||
          !finiteNumber(packet.z, -22.5, 22.5) || !finiteNumber(packet.yaw, -1000, 1000)) return;
      client.state = { x: packet.x, y: packet.y, z: packet.z, yaw: packet.yaw };
      broadcast(world, { type: 'player:state', id: client.id, name: client.name, color: client.color, ...client.state }, ws);
      return;
    }

    if (packet.type === 'chat') {
      const now = Date.now();
      if (now - ws.lastChatAt < 650) return;
      ws.lastChatAt = now;
      const text = String(packet.text ?? '').replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, 180);
      if (text) broadcast(world, { type: 'chat', id: client.id, name: client.name, text, time: now });
      return;
    }

    if (packet.type === 'world:publish') {
      if (!ws.canEdit) { sendError(ws, 'Only the creator can publish this experience.'); return; }
      const previousTitle = world.title;
      const previousUpdatedAt = world.updatedAt;
      const title = cleanTitle(packet.title || world.title);
      world.title = title;
      world.published = true;
      world.updatedAt = new Date().toISOString();
      try {
        await persistWorlds();
        const published = publicWorldRecord(world);
        broadcast(world, { type: 'world:published', world: published });
        broadcastCatalogChanged();
        console.log(`Published "${world.title}" (${world.id}) with ${world.blocks.size} cubes`);
      } catch (err) {
        world.published = false;
        world.title = previousTitle;
        world.updatedAt = previousUpdatedAt;
        console.error('Could not save published world:', err);
        sendError(ws, 'Publish failed: the server could not write the world save. Check its storage configuration.');
      }
      return;
    }

    if (packet.type === 'block:set') {
      if (!ws.canEdit) { sendError(ws, 'You can play this experience, but only its creator can edit it.'); return; }
      const now = Date.now();
      if (now - ws.editWindowAt > 5000) { ws.editWindowAt = now; ws.editCount = 0; }
      ws.editCount++;
      if (ws.editCount > 60) { sendError(ws, 'Too many build edits. Slow down.'); return; }
      const b = packet.block;
      if (!b || typeof b.id !== 'string' || !/^[a-zA-Z0-9_-]{1,100}$/.test(b.id) ||
          !finiteNumber(b.x, -22, 22) || !finiteNumber(b.y, 1, 101) || !finiteNumber(b.z, -22, 22) ||
          !finiteNumber(b.yaw, -1000, 1000) || !Number.isInteger(b.color) || b.color < 0 || b.color > 0xffffff) {
        sendError(ws, 'Invalid cube data.'); return;
      }
      if (!world.blocks.has(b.id) && world.blocks.size >= MAX_BLOCKS_PER_WORLD) {
        sendError(ws, `This experience has reached its ${MAX_BLOCKS_PER_WORLD}-cube limit.`); return;
      }
      const cleanBlock = { id: b.id, x: b.x, y: b.y, z: b.z, yaw: b.yaw, color: b.color };
      const previousBlock = world.blocks.get(b.id);
      const previousUpdatedAt = world.updatedAt;
      world.blocks.set(b.id, cleanBlock);
      world.updatedAt = new Date().toISOString();
      if (world.published) {
        try { await persistWorlds(); }
        catch (err) {
          if (previousBlock) world.blocks.set(b.id, previousBlock); else world.blocks.delete(b.id);
          world.updatedAt = previousUpdatedAt;
          console.error('Could not save cube update:', err); sendError(ws, 'Cube update was not saved. Check server storage.'); return;
        }
      }
      broadcast(world, { type: 'block:set', block: cleanBlock });
      if (world.published) broadcastCatalogChanged();
      return;
    }

    if (packet.type === 'block:delete') {
      if (!ws.canEdit) { sendError(ws, 'Only the creator can edit this experience.'); return; }
      const id = String(packet.id ?? '');
      if (!/^[a-zA-Z0-9_-]{1,100}$/.test(id)) return;
      const previousBlock = world.blocks.get(id);
      if (world.blocks.delete(id)) {
        const previousUpdatedAt = world.updatedAt;
        world.updatedAt = new Date().toISOString();
        if (world.published) {
          try { await persistWorlds(); }
          catch (err) {
            world.blocks.set(id, previousBlock);
            world.updatedAt = previousUpdatedAt;
            console.error('Could not save cube deletion:', err); sendError(ws, 'Cube deletion was not saved. Check server storage.'); return;
          }
        }
        broadcast(world, { type: 'block:delete', id });
        if (world.published) broadcastCatalogChanged();
      }
      return;
    }

    sendError(ws, 'Unknown message type.');
  });

  ws.on('close', () => {
    const world = ws.world;
    const client = ws.client;
    if (!world || !client) return;
    world.clients.delete(client.id);
    broadcast(world, { type: 'player:left', id: client.id, name: client.name });
    broadcast(world, { type: 'room:count', count: world.clients.size });
    broadcastCatalogChanged();
    console.log(`${client.name} left ${world.id} (${world.clients.size}/${MAX_PLAYERS_PER_WORLD})`);
    // Published worlds and their cubes remain on disk with zero online players.
  });
});

const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 25_000);

await loadWorlds();
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Blockyard listening on http://localhost:${PORT}`);
  console.log(`WebSocket endpoint: ws://localhost:${PORT}/ws`);
  console.log(`Persistent world data: ${DATA_FILE}`);
});

async function shutdown() {
  clearInterval(heartbeat);
  for (const ws of wss.clients) ws.close(1001, 'Server shutting down');
  try { await persistWorlds(); } catch (err) { console.error('Final world save failed:', err); }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 3000).unref();
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
