require('dotenv').config();
/* eslint-disable no-process-exit */

const http = require('http');
const { WebSocketServer } = require('ws');
const admin = require('firebase-admin');

const pkg = require('./package');
const app = require('./app');
const streamStore = require('./streamStore');
const { initFirebase } = require('./firebaseActions');
const { startSessionIfNeeded } = require('./sessionManager');

const PORT = parseInt(process.env.PORT, 10) || 8080;

bootstrapFirebase();

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

  ws.on('message', async (data, isBinary) => {
    try {
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

      if (!ws.deviceId) {
        console.warn('Dropping frame because deviceId was not set first');
        return;
      }

      const jpegBuffer = Buffer.from(data);
      streamStore.setLatestFrame(ws.deviceId, jpegBuffer);
      await startSessionIfNeeded(ws.deviceId);
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

function bootstrapFirebase() {
  const dbUrl = process.env.FIREBASE_DB_URL;
  const saJson = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;

  if (!dbUrl || !saJson) {
    console.warn('Firebase disabled: FIREBASE_DB_URL and FIREBASE_SERVICE_ACCOUNT_JSON not set');
    return;
  }

  try {
    const serviceAccount = JSON.parse(saJson);
    if (!admin.apps.length) {
      admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
        databaseURL: dbUrl,
      });
    }

    initFirebase(admin, admin.database());
    console.log('Firebase initialized');
  } catch (err) {
    console.error('Failed to initialize Firebase:', err);
  }
}

module.exports = server;
