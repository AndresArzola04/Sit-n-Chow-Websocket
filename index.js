

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
    streamStore.setLatestFrame(Buffer.from(data));
  });

  ws.on('error', (err) => console.error('WS error:', err));
});

server.listen(PORT, () =>
  console.log(`${pkg.name}: listening on port ${PORT}`)
);

process.on('SIGTERM', () => {
  console.log(`${pkg.name}: received SIGTERM`);
  try { wss.close(); } catch (e) {}
  server.close(() => process.exit(0));
});

module.exports = server;