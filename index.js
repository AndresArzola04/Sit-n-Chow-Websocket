

/* eslint-disable no-process-exit */

const http = require('http');
const {WebSocketServer} = require('ws');

const pkg = require('./package');
const app = require('./app');
const streamStore = require('./streamStore');

const PORT = parseInt(process.env.PORT, 10) || 8080;

const server = http.createServer(app);

const wss = new WebSocketServer({server, path: '/ingest'});

wss.on('connection', (ws) => {
  ws.on('message', (data, isBinary) => {
    if (!isBinary) return;
    // Copy the buffer immediately — ws reuses its internal buffer
    streamStore.setLatestFrame(Buffer.from(data));
  });

  // Add a ping to detect dead ESP32 connections
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('error', (err) => console.error('WS error:', err));
});

// Heartbeat — kill dead connections so Cloud Run doesn't hold them open
const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, 15000);

wss.on('close', () => clearInterval(heartbeat));

server.listen(PORT, () =>
  console.log(`${pkg.name}: listening on port ${PORT}`)
);

process.on('SIGTERM', () => {
  console.log(`${pkg.name}: received SIGTERM`);
  try { wss.close(); } catch (e) {}
  server.close(() => process.exit(0));
});

module.exports = server;