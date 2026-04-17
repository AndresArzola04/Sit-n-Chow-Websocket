require('dotenv').config();

/* eslint-disable no-process-exit */

const http = require('http');
const express = require('express');
const admin = require('firebase-admin');
const { WebSocketServer } = require('ws');

const streamStore = require('./streamStore');
const {
  startSession,
  stopSession,
  getPublicSession,
  listSessions,
} = require('./sessionManager');
const { initFirebase } = require('./firebaseActions');

const PORT = parseInt(process.env.PORT || '8080', 10);

bootstrapFirebase();

const app = express();
app.use(express.json());

app.get('/health', (_req, res) => {
  res.json({ status: 'ok' });
});

app.get('/view', (req, res) => {
  const deviceId = req.query.device;
  if (!deviceId) {
    return res.status(400).send('Missing device query parameter');
  }

  const { latestJpeg } = streamStore.getLatestFrame(deviceId);
  if (!latestJpeg) {
    return res.status(404).send('No frame available for device');
  }

  res.setHeader('Content-Type', 'image/jpeg');
  res.setHeader('Cache-Control', 'no-store');
  return res.end(latestJpeg);
});

app.get('/stream.mjpeg', (req, res) => {
  const deviceId = req.query.device;
  if (!deviceId) {
    return res.status(400).send('Missing device query parameter');
  }

  res.writeHead(200, {
    'Content-Type': 'multipart/x-mixed-replace; boundary=frame',
    'Cache-Control': 'no-store, no-cache, must-revalidate, proxy-revalidate',
    Pragma: 'no-cache',
    Expires: '0',
    Connection: 'close',
  });

  let closed = false;

  const interval = setInterval(() => {
    if (closed) return;

    const { latestJpeg } = streamStore.getLatestFrame(deviceId);
    if (!latestJpeg) return;

    res.write(`--frame\r\n`);
    res.write(`Content-Type: image/jpeg\r\n`);
    res.write(`Content-Length: ${latestJpeg.length}\r\n\r\n`);
    res.write(latestJpeg);
    res.write('\r\n');
  }, 200);

  req.on('close', () => {
    closed = true;
    clearInterval(interval);
  });
});

app.post('/sessions/:deviceId/start', async (req, res) => {
  try {
    const { deviceId } = req.params;

    const successSeconds = parseOptionalNumber(
      req.query.success_seconds ?? req.body?.success_seconds
    );
    const timeoutSeconds = parseOptionalNumber(
      req.query.timeout_seconds ?? req.body?.timeout_seconds
    );
    const grams = parseOptionalInteger(
      req.query.grams ?? req.body?.grams
    );

    const session = await startSession(deviceId, {
      successSeconds,
      timeoutSeconds,
      grams,
    });

    return res.json({
      ok: true,
      message: 'Session started',
      session,
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: String(err?.message || err),
    });
  }
});

app.post('/sessions/:deviceId/stop', async (req, res) => {
  try {
    const { deviceId } = req.params;
    await stopSession(deviceId, {
      type: 'session_stopped',
      reason: 'manual_stop',
    });

    return res.json({
      ok: true,
      message: 'Session stopped',
      deviceId,
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: String(err?.message || err),
    });
  }
});

app.get('/session', (req, res) => {
  const deviceId = req.query.device;
  if (!deviceId) {
    return res.status(400).json({ error: 'Missing device query parameter' });
  }

  const session = getPublicSession(deviceId);
  if (!session) {
    return res.status(404).json({ error: 'No session found for device', deviceId });
  }

  return res.json(session);
});

app.get('/sessions', (_req, res) => {
  res.json(listSessions());
});

const server = http.createServer(app);

const wss = new WebSocketServer({
  server,
  path: '/ingest',
});

wss.on('connection', (ws) => {
  ws.deviceId = null;
  ws.autoSessionStarted = false;

  ws.on('message', async (data, isBinary) => {
    try {
      if (!isBinary) {
        const text = data.toString('utf8');

        let msg;
        try {
          msg = JSON.parse(text);
        } catch {
          return;
        }

        if (msg?.type === 'hello' && msg?.deviceId) {
          ws.deviceId = msg.deviceId;
          await startSessionOnce(ws);
        }

        return;
      }

      if (!ws.deviceId) {
        return;
      }

      const frameBuffer = Buffer.isBuffer(data) ? data : Buffer.from(data);
      streamStore.setLatestFrame(ws.deviceId, frameBuffer);

      await startSessionOnce(ws);
    } catch (err) {
      console.error('[ws] message handling error:', err);
    }
  });

  ws.on('error', (err) => {
    console.error('[ws] socket error', {
      deviceId: ws.deviceId,
      error: err.message,
    });
  });
});

server.listen(PORT, () => {
  console.log(`websockets: listening on port ${PORT}`);
});

function bootstrapFirebase() {
  const dbUrl = process.env.FIREBASE_DB_URL;
  const rawJson = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;

  const rawJson =process.env.FIREBASE_SERVICE_ACCOUNT_JSON || process.env.FIREBASE_SERVICE_ACCOUNT;

  console.log('FIREBASE_DB_URL exists:', !!process.env.FIREBASE_DB_URL);
  console.log('FIREBASE_SERVICE_ACCOUNT exists:', !!process.env.FIREBASE_SERVICE_ACCOUNT);
  console.log('FIREBASE_SERVICE_ACCOUNT_JSON exists:', !!process.env.FIREBASE_SERVICE_ACCOUNT_JSON);

  if (!dbUrl || !rawJson) {
    console.log('Firebase not configured; running without Firebase');
    return;
  }

  try {
    const serviceAccount = JSON.parse(rawJson);

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

async function startSessionOnce(ws) {
  if (!ws?.deviceId) return;
  if (ws.autoSessionStarted) return;

  const existing = getPublicSession(ws.deviceId);
  if (existing?.active) {
    ws.autoSessionStarted = true;
    return;
  }

  try {
    await startSession(ws.deviceId, {
      successSeconds: 5,
      timeoutSeconds: 180,
    });
    ws.autoSessionStarted = true;
  } catch (err) {
    console.error('[session] auto-start failed:', err);
  }
}

function parseOptionalNumber(value) {
  if (value === undefined || value === null || value === '') return undefined;
  const n = Number(value);
  return Number.isFinite(n) ? n : undefined;
}

function parseOptionalInteger(value) {
  if (value === undefined || value === null || value === '') return undefined;
  const n = parseInt(String(value), 10);
  return Number.isFinite(n) ? n : undefined;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}