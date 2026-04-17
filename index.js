require('dotenv').config();

/* eslint-disable no-process-exit */

const http = require('http');
const fs = require('fs');
const path = require('path');
const express = require('express');
const admin = require('firebase-admin');
const { WebSocketServer } = require('ws');

const pkg = require('./package');
const app = express();

const streamStore = require('./streamStore');
const audioStore = require('./audioStore');

const {
  startSession,
  stopSession,
  getPublicSession,
  listSessions,
} = require('./sessionManager');

const { initFirebase } = require('./firebaseActions');

const PORT = parseInt(process.env.PORT || '8080', 10);

/* ── Express setup ───────────────────────────────────────────────────────── */

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

/* ── Session routes ──────────────────────────────────────────────────────── */

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

/* ── Firebase Admin SDK ──────────────────────────────────────────────────── */

let firebaseDb = null;

function bootstrapFirebase() {
  const dbUrl =
    process.env.FIREBASE_DB_URL ||
    process.env.FIREBASE_DATABASE_URL;

  const rawJson =
    process.env.FIREBASE_SERVICE_ACCOUNT_JSON ||
    process.env.FIREBASE_SERVICE_ACCOUNT;

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

    firebaseDb = admin.database();
    initFirebase(admin, firebaseDb);
    console.log('Firebase initialized');
  } catch (err) {
    console.error('Failed to initialize Firebase:', err);
  }
}

bootstrapFirebase();

/* ── PCM utilities ───────────────────────────────────────────────────────── */

const TARGET_RATE = 16000;

function swapBytes(buf) {
  const out = Buffer.allocUnsafe(buf.length);
  for (let i = 0; i + 1 < buf.length; i += 2) {
    out[i] = buf[i + 1];
    out[i + 1] = buf[i];
  }
  return out;
}

function resamplePCM(inputBuf, srcRate) {
  if (srcRate === TARGET_RATE) return inputBuf;

  const srcSamples = Math.floor(inputBuf.length / 2);
  const ratio = srcRate / TARGET_RATE;
  const dstSamples = Math.floor(srcSamples / ratio);
  const outputBuf = Buffer.allocUnsafe(dstSamples * 2);

  for (let i = 0; i < dstSamples; i++) {
    const srcPos = i * ratio;
    const srcIdx = Math.floor(srcPos);
    const frac = srcPos - srcIdx;
    const s0 = inputBuf.readInt16LE(srcIdx * 2);
    const s1 = (srcIdx + 1 < srcSamples)
      ? inputBuf.readInt16LE((srcIdx + 1) * 2)
      : s0;
    const out = Math.round(s0 + frac * (s1 - s0));
    outputBuf.writeInt16LE(Math.max(-32768, Math.min(32767, out)), i * 2);
  }

  return outputBuf;
}

function processAudio(buf) {
  const GAIN = 4.0;
  const SILENCE_RMS = 80;
  const samples = buf.length / 2;

  let sumSq = 0;
  for (let i = 0; i < samples; i++) {
    const s = buf.readInt16LE(i * 2);
    sumSq += s * s;
  }
  if (Math.sqrt(sumSq / samples) < SILENCE_RMS) {
    return Buffer.alloc(buf.length, 0);
  }

  const out = Buffer.allocUnsafe(buf.length);
  for (let i = 0; i < samples; i++) {
    const s = buf.readInt16LE(i * 2);
    const boosted = s * GAIN;
    const clipped = Math.tanh(boosted / 32768) * 32767;
    out.writeInt16LE(Math.round(clipped), i * 2);
  }
  return out;
}

/* ── WAV capture for debug-audio endpoint ───────────────────────────────── */

let debugChunks = [];
let debugCapturing = false;

function writeWav(pcmBuf, sampleRate, outputPath) {
  const numChannels = 1;
  const bitsPerSample = 16;
  const byteRate = sampleRate * numChannels * bitsPerSample / 8;
  const blockAlign = numChannels * bitsPerSample / 8;
  const dataSize = pcmBuf.length;
  const header = Buffer.allocUnsafe(44);

  header.write('RIFF', 0);
  header.writeUInt32LE(36 + dataSize, 4);
  header.write('WAVE', 8);
  header.write('fmt ', 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20);
  header.writeUInt16LE(numChannels, 22);
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(byteRate, 28);
  header.writeUInt16LE(blockAlign, 32);
  header.writeUInt16LE(bitsPerSample, 34);
  header.write('data', 36);
  header.writeUInt32LE(dataSize, 40);

  fs.writeFileSync(outputPath, Buffer.concat([header, pcmBuf]));
  console.log(`Debug WAV saved: ${pcmBuf.length} bytes → ${outputPath}`);
}

/* ── ESP32 token endpoint ────────────────────────────────────────────────── */

app.get('/esp-token', async (req, res) => {
  if (!admin.apps.length || !firebaseDb) {
    return res.status(503).json({ error: 'Firebase not initialised' });
  }

  const deviceId = req.query.device;
  if (!deviceId || typeof deviceId !== 'string' || deviceId.length > 64) {
    return res.status(400).json({ error: 'Missing or invalid device param' });
  }

  const secret = process.env.DEVICE_SECRET;
  if (secret && req.headers['x-device-secret'] !== secret) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    const snap = await firebaseDb.ref(`devices/${deviceId}/ownerUid`).once('value');
    if (!snap.exists()) {
      return res.status(404).json({ error: 'Device not registered' });
    }

    const customToken = await admin.auth().createCustomToken(deviceId, { role: 'device' });
    const apiKey = process.env.FIREBASE_API_KEY;
    if (!apiKey) {
      return res.status(500).json({ error: 'Server misconfigured' });
    }

    const authRes = await fetch(
      `https://identitytoolkit.googleapis.com/v1/accounts:signInWithCustomToken?key=${apiKey}`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token: customToken, returnSecureToken: true }),
      }
    );

    const authData = await authRes.json();
    if (!authRes.ok || !authData.idToken) {
      console.error('ID token exchange failed:', authData);
      return res.status(500).json({ error: 'Token exchange failed' });
    }

    return res.json({ token: authData.idToken, expiresIn: 3600 });
  } catch (err) {
    console.error('Token mint error:', err);
    return res.status(500).json({ error: 'Token generation failed' });
  }
});

/* ── Notification forwarder ──────────────────────────────────────────────── */

function startNotificationForwarder() {
  if (!firebaseDb || !admin.apps.length) return;

  firebaseDb.ref('notifications').on('child_added', async (uidSnap) => {
    const uid = uidSnap.key;

    uidSnap.ref.on('child_added', async (notifSnap) => {
      const notif = notifSnap.val();
      if (!notif || !notif.title) return;

      await notifSnap.ref.remove();

      const tokensSnap = await firebaseDb.ref(`userFcmTokens/${uid}`).once('value');
      if (!tokensSnap.exists()) return;

      const tokens = Object.keys(tokensSnap.val());
      if (tokens.length === 0) return;

      const message = {
        notification: { title: notif.title, body: notif.body || '' },
        data: { deviceId: notif.deviceId || '', ts: String(notif.ts || Date.now()) },
        tokens,
      };

      try {
        const response = await admin.messaging().sendEachForMulticast(message);
        console.log(
          `Notification sent to ${uid}: ${response.successCount} ok, ${response.failureCount} failed`
        );

        response.responses.forEach((r, i) => {
          if (
            !r.success &&
            r.error &&
            (
              r.error.code === 'messaging/registration-token-not-registered' ||
              r.error.code === 'messaging/invalid-registration-token'
            )
          ) {
            firebaseDb.ref(`userFcmTokens/${uid}/${tokens[i]}`).remove();
          }
        });
      } catch (err) {
        console.error('FCM send error:', err.message);
      }
    });
  });

  console.log('Notification forwarder listening');
}

startNotificationForwarder();

/* ── HTTP + WebSocket server ─────────────────────────────────────────────── */

const server = http.createServer(app);

/* ── /ingest — camera frames with deviceId/session flow ─────────────────── */

const wssCamera = new WebSocketServer({ noServer: true });

wssCamera.on('connection', (ws) => {
  ws.deviceId = null;
  ws.autoSessionStarted = false;
  ws.isAlive = true;

  ws.on('pong', () => {
    ws.isAlive = true;
  });

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

      if (!ws.deviceId) return;

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

  ws.on('close', () => {
    console.log('Camera ingest disconnected', { deviceId: ws.deviceId });
  });
});

/* ── /audio-ingest — Flutter mic → server ───────────────────────────────── */

const wssAudioIngest = new WebSocketServer({ noServer: true });

wssAudioIngest.on('connection', (ws) => {
  console.log('Audio ingest connected (Flutter mic active)');
  audioStore.clearChunks();

  debugChunks = [];
  debugCapturing = true;

  ws.isAlive = true;
  ws.on('pong', () => {
    ws.isAlive = true;
  });

  let srcRate = null;
  let bigEndian = false;

  ws.on('message', (data, isBinary) => {
    if (!isBinary) {
      const msg = data.toString().trim();

      if (msg.startsWith('sampleRate:')) {
        const rate = parseInt(msg.split(':')[1], 10);
        if (rate > 0 && rate <= 192000) {
          srcRate = rate;
          console.log(`Audio ingest: sample rate set to ${srcRate} Hz`);
        }
        return;
      }

      if (msg.startsWith('bigEndian:')) {
        bigEndian = msg.split(':')[1].trim() === 'true';
        console.log(`Audio ingest: bigEndian=${bigEndian}`);
        return;
      }

      if (msg === 'stop') {
        console.log('Audio ingest: stop signal received');
        audioStore.clearChunks();

        wssAudioStream.clients.forEach((client) => {
          if (client.readyState === client.OPEN) {
            client.send('stop', { binary: false });
          }
        });

        if (debugCapturing && debugChunks.length > 0) {
          try {
            const combined = Buffer.concat(debugChunks);
            writeWav(combined, TARGET_RATE, path.join('/tmp', 'debug_audio.wav'));
          } catch (e) {
            console.error('Failed to write debug WAV:', e.message);
          }
          debugChunks = [];
          debugCapturing = false;
        }
        return;
      }

      console.warn(`Audio ingest: unknown text message "${msg}"`);
      return;
    }

    if (srcRate === null) {
      console.warn('Audio ingest: received PCM before sampleRate handshake, dropping');
      return;
    }

    let rawBuf = Buffer.from(data);

    if (bigEndian) rawBuf = swapBytes(rawBuf);

    const outBuf = processAudio(resamplePCM(rawBuf, srcRate));

    if (debugCapturing) {
      debugChunks.push(Buffer.from(rawBuf));
    }

    audioStore.setLatestChunk(outBuf);

    wssAudioStream.clients.forEach((client) => {
      if (client.readyState === client.OPEN) {
        client.send(outBuf, { binary: true });
      }
    });
  });

  ws.on('close', () => {
    console.log('Audio ingest disconnected');
    audioStore.clearChunks();

    wssAudioStream.clients.forEach((client) => {
      if (client.readyState === client.OPEN) {
        client.send('stop', { binary: false });
      }
    });

    if (debugCapturing && debugChunks.length > 0) {
      try {
        const combined = Buffer.concat(debugChunks);
        writeWav(combined, TARGET_RATE, path.join('/tmp', 'debug_audio.wav'));
      } catch (e) {
        console.error('Failed to write debug WAV:', e.message);
      }
      debugChunks = [];
      debugCapturing = false;
    }
  });

  ws.on('error', (err) => console.error('Audio ingest WS error:', err));
});

/* ── /audio-stream — server → ESP32 ─────────────────────────────────────── */

const wssAudioStream = new WebSocketServer({ noServer: true });

wssAudioStream.on('connection', (ws) => {
  console.log('ESP32 audio stream connected');
  ws.isAlive = true;

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('error', (err) => console.error('Audio stream WS error:', err));
  ws.on('close', () => console.log('ESP32 audio stream disconnected'));
});

/* ── Upgrade router ──────────────────────────────────────────────────────── */

server.on('upgrade', (request, socket, head) => {
  const { pathname } = new URL(request.url, `http://${request.headers.host}`);

  if (pathname === '/ingest') {
    wssCamera.handleUpgrade(request, socket, head, (ws) => {
      wssCamera.emit('connection', ws, request);
    });
  } else if (pathname === '/audio-ingest') {
    wssAudioIngest.handleUpgrade(request, socket, head, (ws) => {
      wssAudioIngest.emit('connection', ws, request);
    });
  } else if (pathname === '/audio-stream') {
    wssAudioStream.handleUpgrade(request, socket, head, (ws) => {
      wssAudioStream.emit('connection', ws, request);
    });
  } else {
    socket.destroy();
  }
});

/* ── Heartbeat ───────────────────────────────────────────────────────────── */

function heartbeatAll(wss) {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) {
      ws.terminate();
      return;
    }
    ws.isAlive = false;
    ws.ping();
  });
}

const heartbeat = setInterval(() => {
  heartbeatAll(wssCamera);
  heartbeatAll(wssAudioStream);
  heartbeatAll(wssAudioIngest);
}, 15000);

server.on('close', () => clearInterval(heartbeat));

server.listen(PORT, () => {
  console.log(`${pkg.name || 'websockets'}: listening on port ${PORT}`);
});

process.on('SIGTERM', () => {
  console.log(`${pkg.name || 'websockets'}: received SIGTERM`);
  try { wssCamera.close(); } catch (e) {}
  try { wssAudioIngest.close(); } catch (e) {}
  try { wssAudioStream.close(); } catch (e) {}
  server.close(() => process.exit(0));
});

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

module.exports = server;