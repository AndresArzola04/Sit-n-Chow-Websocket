/* eslint-disable no-process-exit */

const http = require('http');
const { WebSocketServer } = require('ws');

const pkg = require('./package');
const app = require('./app');
const streamStore = require('./streamStore');

const PORT = parseInt(process.env.PORT, 10) || 8080;

console.log("USING LOCAL TEST INDEX.JS");

process.on('uncaughtException', (err) => {
  console.error('UNCAUGHT EXCEPTION:', err);
});

process.on('unhandledRejection', (err) => {
  console.error('UNHANDLED REJECTION:', err);
});

const server = http.createServer(app);

server.on('error', (err) => {
  console.error('HTTP SERVER ERROR:', err);
});

const wss = new WebSocketServer({ server, path: '/ingest' });

wss.on('connection', (ws, req) => {
  console.log('WS connection opened from', req.socket.remoteAddress);

  ws.deviceId = null;

  ws.on('message', (data, isBinary) => {
    try {
      // TEXT MESSAGE (device registration)
      if (!isBinary) {
        const txt = data.toString();
        console.log('WS text message:', txt);

        const msg = JSON.parse(txt);

        if (
          msg.type === 'hello' &&
          typeof msg.deviceId === 'string' &&
          msg.deviceId.length > 0
        ) {
          ws.deviceId = msg.deviceId;
          console.log(`WS registered deviceId=${ws.deviceId}`);
          return;
        }

        return;
      }

      // BINARY FRAME
      if (!ws.deviceId) {
        console.warn('Dropping frame because deviceId was not set first');
        return;
      }

      console.log(`Received frame for ${ws.deviceId}, bytes=${data.length}`);

      const jpegBuffer = Buffer.from(data);

      streamStore.setLatestFrame(ws.deviceId, jpegBuffer);

      console.log(`Stored latest frame for ${ws.deviceId}`);
    } catch (err) {
      console.error('WS message handler error:', err);
    }
  });

  ws.on('error', (err) => {
    console.error('WS error:', err);
  });

  ws.on('close', (code, reason) => {
    console.log(
      `WS closed for deviceId=${ws.deviceId}, code=${code}, reason=${reason.toString()}`
    );
  });
});

server.listen(PORT, () => {
  console.log(`${pkg.name}: listening on port ${PORT}`);
});

process.on('SIGTERM', () => {
  console.log(`${pkg.name}: received SIGTERM`);
  try {
    wss.close();
  } catch (e) {}
  server.close(() => process.exit(0));
});

module.exports = server;