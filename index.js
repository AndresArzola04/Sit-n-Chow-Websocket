/* eslint-disable no-process-exit */

const http = require('http');
const fs = require('fs');
const path = require('path');
const admin = require('firebase-admin');
const { WebSocketServer } = require('ws');

const app = require('./app');
const streamStore = require('./streamStore');
const audioStore = require('./audioStore');
const {
  startSession,
  stopSession,
  getPublicSession,
} = require('./sessionManager');
const { initFirebase } = require('./firebaseActions');

const PORT = parseInt(process.env.PORT || '8080', 10);
const TARGET_RATE = 16000;

let debugChunks = [];
let debugCapturing = false;

bootstrapFirebase();

app.use(require('express').json());

app.get('/esp-token', async (req, res) => {
  try {
    const deviceId = String(req.query.device || '').trim();
    if (!deviceId) {
      return res.status(400).json({ ok: false, error: 'Missing device query param' });
    }

    const expectedSecret = process.env.ESP_DEVICE_SECRET || '';
    if (expectedSecret) {
      const providedSecret = req.get('X-Device-Secret') || '';
      if (providedSecret !== expectedSecret) {
        return res.status(403).json({ ok: false, error: 'Forbidden' });
      }
    }

    if (!admin.apps.length) {
      return res.status(503).json({ ok: false, error: 'Firebase is not configured' });
    }

    const token = await admin.auth().createCustomToken(deviceId, {
      deviceId,
      role: 'device',
    });

    return res.json({ ok: true, token, deviceId });
  } catch (err) {
    console.error('[esp-token] error:', err);
    return res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
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

const server = http.createServer(app);

const wssCamera = new WebSocketServer({ noServer: true });
const wssAudioIngest = new WebSocketServer({ noServer: true });
const wssAudioStream = new WebSocketServer({ noServer: true });

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

      if (!ws.deviceId) {
        return;
      }

      const frameBuffer = Buffer.isBuffer(data) ? data : Buffer.from(data);
      streamStore.setLatestFrame(ws.deviceId, frameBuffer);
      await startSessionOnce(ws);
    } catch (err) {
      console.error('[camera ws] message handling error:', err);
    }
  });

  ws.on('error', (err) => {
    console.error('[camera ws] socket error', {
      deviceId: ws.deviceId,
      error: err.message,
    });
  });
});

wssAudioIngest.on('connection', (ws) => {
  console.log('Audio ingest connected');
  audioStore.clearChunks();
  debugChunks = [];
  debugCapturing = true;

  let srcRate = null;
  let bigEndian = false;
  ws.isAlive = true;

  ws.on('pong', () => {
    ws.isAlive = true;
  });

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
          if (client.readyState === client.OPEN) client.send('stop', { binary: false });
        });
        flushDebugAudio();
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

    if (debugCapturing) debugChunks.push(Buffer.from(rawBuf));

    const outBuf = processAudio(resamplePCM(rawBuf, srcRate));
    audioStore.setLatestChunk(outBuf);
    wssAudioStream.clients.forEach((client) => {
      if (client.readyState === client.OPEN) client.send(outBuf, { binary: true });
    });
  });

  ws.on('close', () => {
    console.log('Audio ingest disconnected');
    audioStore.clearChunks();
    wssAudioStream.clients.forEach((client) => {
      if (client.readyState === client.OPEN) client.send('stop', { binary: false });
    });
    flushDebugAudio();
  });

  ws.on('error', (err) => console.error('Audio ingest WS error:', err));
});

wssAudioStream.on('connection', (ws) => {
  console.log('ESP32 audio stream connected');
  ws.isAlive = true;
  ws.on('pong', () => {
    ws.isAlive = true;
  });
  ws.on('error', (err) => console.error('Audio stream WS error:', err));
  ws.on('close', () => console.log('ESP32 audio stream disconnected'));
});

server.on('upgrade', (request, socket, head) => {
  const { pathname } = new URL(request.url, `http://${request.headers.host}`);

  if (pathname === '/ingest') {
    wssCamera.handleUpgrade(request, socket, head, (ws) => wssCamera.emit('connection', ws, request));
  } else if (pathname === '/audio-ingest') {
    wssAudioIngest.handleUpgrade(request, socket, head, (ws) => wssAudioIngest.emit('connection', ws, request));
  } else if (pathname === '/audio-stream') {
    wssAudioStream.handleUpgrade(request, socket, head, (ws) => wssAudioStream.emit('connection', ws, request));
  } else {
    socket.destroy();
  }
});

const heartbeat = setInterval(() => {
  heartbeatAll(wssCamera);
  heartbeatAll(wssAudioIngest);
  heartbeatAll(wssAudioStream);
}, 15000);

server.on('close', () => clearInterval(heartbeat));

server.listen(PORT, () => {
  console.log(`websockets: listening on port ${PORT}`);
});

process.on('SIGTERM', () => {
  console.log('websockets: received SIGTERM');
  try { wssCamera.close(); } catch {}
  try { wssAudioIngest.close(); } catch {}
  try { wssAudioStream.close(); } catch {}
  server.close(() => process.exit(0));
});

function bootstrapFirebase() {
  const dbUrl = process.env.FIREBASE_DB_URL;
  const rawJson = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;

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

    const idx1 = Math.min(srcIdx, srcSamples - 1);
    const idx2 = Math.min(srcIdx + 1, srcSamples - 1);

    const s1 = inputBuf.readInt16LE(idx1 * 2);
    const s2 = inputBuf.readInt16LE(idx2 * 2);
    const interpolated = Math.round(s1 + (s2 - s1) * frac);
    outputBuf.writeInt16LE(interpolated, i * 2);
  }

  return outputBuf;
}

function processAudio(buf) {
  return buf;
}

function flushDebugAudio() {
  if (!debugCapturing || debugChunks.length === 0) {
    debugChunks = [];
    debugCapturing = false;
    return;
  }

  try {
    const combined = Buffer.concat(debugChunks);
    writeWav(combined, TARGET_RATE, path.join('/tmp', 'debug_audio.wav'));
  } catch (e) {
    console.error('Failed to write debug WAV:', e.message);
  }
  debugChunks = [];
  debugCapturing = false;
}

function writeWav(pcm16leBuf, sampleRate, filePath) {
  const numChannels = 1;
  const bitsPerSample = 16;
  const blockAlign = numChannels * bitsPerSample / 8;
  const byteRate = sampleRate * blockAlign;
  const dataSize = pcm16leBuf.length;
  const header = Buffer.alloc(44);

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

  fs.writeFileSync(filePath, Buffer.concat([header, pcm16leBuf]));
}
