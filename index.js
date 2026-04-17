/* eslint-disable no-process-exit */

const http = require('http');
const fs   = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const pkg         = require('./package.json');
const app         = require('./app');
const streamStore = require('./streamStore');
const audioStore  = require('./audioStore');
const { initFirebase }                              = require('./firebaseActions');
const { startSession, getPublicSession }            = require('./sessionManager');

const PORT = parseInt(process.env.PORT, 10) || 8080;

/* ── Firebase Admin SDK ──────────────────────────────────────────────────── */

let admin = null;
let db    = null;

function initFirebaseAdmin() {
  try {
    admin = require('firebase-admin');

    // Support both env-var naming conventions (Firebase repo vs ML repo)
    const raw   = process.env.FIREBASE_SERVICE_ACCOUNT || process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
    const dbUrl = process.env.FIREBASE_DATABASE_URL    || process.env.FIREBASE_DB_URL;

    if (!raw) {
      console.warn('FIREBASE_SERVICE_ACCOUNT not set — Firebase features disabled');
      return;
    }

    const serviceAccount = JSON.parse(raw);

    if (!admin.apps.length) {
      admin.initializeApp({
        credential:  admin.credential.cert(serviceAccount),
        databaseURL: dbUrl,
      });
    }

    db = admin.database();

    // Share the initialised admin + db with the firebaseActions module so that
    // sessionManager can write feed events and ML status updates.
    initFirebase(admin, db);

    console.log('Firebase Admin SDK initialised');
  } catch (err) {
    console.error('Firebase Admin init failed:', err.message);
  }
}

initFirebaseAdmin();

/* ── PCM utilities ───────────────────────────────────────────────────────── */

const TARGET_RATE = 16000;

function swapBytes(buf) {
  const out = Buffer.allocUnsafe(buf.length);
  for (let i = 0; i + 1 < buf.length; i += 2) {
    out[i]     = buf[i + 1];
    out[i + 1] = buf[i];
  }
  return out;
}

function resamplePCM(inputBuf, srcRate) {
  if (srcRate === TARGET_RATE) return inputBuf;

  const srcSamples = Math.floor(inputBuf.length / 2);
  const ratio      = srcRate / TARGET_RATE;
  const dstSamples = Math.floor(srcSamples / ratio);
  const outputBuf  = Buffer.allocUnsafe(dstSamples * 2);

  for (let i = 0; i < dstSamples; i++) {
    const srcPos = i * ratio;
    const srcIdx = Math.floor(srcPos);
    const frac   = srcPos - srcIdx;
    const s0 = inputBuf.readInt16LE(srcIdx * 2);
    const s1 = (srcIdx + 1 < srcSamples)
      ? inputBuf.readInt16LE((srcIdx + 1) * 2)
      : s0;
    const interpolated = Math.round(s0 + frac * (s1 - s0));
    outputBuf.writeInt16LE(Math.max(-32768, Math.min(32767, interpolated)), i * 2);
  }
  return outputBuf;
}

const GATE_THRESHOLD = 300;
const GAIN_FACTOR    = 2.5;

function processAudio(buf) {
  let maxAbs = 0;
  for (let i = 0; i + 1 < buf.length; i += 2) {
    const s = Math.abs(buf.readInt16LE(i));
    if (s > maxAbs) maxAbs = s;
  }
  if (maxAbs < GATE_THRESHOLD) return buf;

  const out = Buffer.allocUnsafe(buf.length);
  for (let i = 0; i + 1 < buf.length; i += 2) {
    const s = buf.readInt16LE(i);
    out.writeInt16LE(Math.max(-32768, Math.min(32767, Math.round(s * GAIN_FACTOR))), i);
  }
  return out;
}

function writeWav(pcmBuf, sampleRate, filePath) {
  const numChannels  = 1;
  const bitsPerSample = 16;
  const byteRate     = sampleRate * numChannels * bitsPerSample / 8;
  const blockAlign   = numChannels * bitsPerSample / 8;
  const dataSize     = pcmBuf.length;
  const header       = Buffer.alloc(44);

  header.write('RIFF', 0, 'ascii');
  header.writeUInt32LE(36 + dataSize, 4);
  header.write('WAVE', 8, 'ascii');
  header.write('fmt ', 12, 'ascii');
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20);           // PCM
  header.writeUInt16LE(numChannels, 22);
  header.writeUInt32LE(sampleRate, 24);
  header.writeUInt32LE(byteRate, 28);
  header.writeUInt16LE(blockAlign, 32);
  header.writeUInt16LE(bitsPerSample, 34);
  header.write('data', 36, 'ascii');
  header.writeUInt32LE(dataSize, 40);

  fs.writeFileSync(filePath, Buffer.concat([header, pcmBuf]));
}

/* ── Debug audio capture state ───────────────────────────────────────────── */

let debugChunks    = [];
let debugCapturing = false;

/* ── Notification forwarder (FCM push) ───────────────────────────────────── */

function startNotificationForwarder() {
  if (!db) return;

  db.ref('pendingNotifications').on('child_added', async (snapshot) => {
    const notif = snapshot.val();
    if (!notif) return;
    snapshot.ref.remove();

    const { uid, title, body } = notif;
    if (!uid || !title || !body) return;

    const tokensSnap = await db.ref(`userFcmTokens/${uid}`).once('value');
    const tokensObj  = tokensSnap.val();
    if (!tokensObj) return;

    const tokens = Object.keys(tokensObj);
    if (!tokens.length) return;

    const message = {
      notification: { title, body },
      data: { deviceId: notif.deviceId || '', ts: String(notif.ts || Date.now()) },
      tokens,
    };
    try {
      const response = await admin.messaging().sendEachForMulticast(message);
      console.log(`Notification sent to ${uid}: ${response.successCount} ok, ${response.failureCount} failed`);
      response.responses.forEach((r, i) => {
        if (!r.success && r.error &&
            (r.error.code === 'messaging/registration-token-not-registered' ||
             r.error.code === 'messaging/invalid-registration-token'))
          db.ref(`userFcmTokens/${uid}/${tokens[i]}`).remove();
      });
    } catch (err) {
      console.error('FCM send error:', err.message);
    }
  });

  console.log('Notification forwarder listening');
}

startNotificationForwarder();

/* ── HTTP + WebSocket server ─────────────────────────────────────────────── */

const server = http.createServer(app);

/* ── /ingest — ESP32 camera frames (deviceId-aware) ─────────────────────── */
//
// The ESP32 sends a JSON `hello` message first to identify itself, then sends
// raw JPEG binary frames. On first frame/hello we auto-start an ML session.

const wssCamera = new WebSocketServer({ noServer: true });

wssCamera.on('connection', (ws) => {
  console.log('Camera ingest connected');
  ws.deviceId          = null;
  ws.autoSessionStarted = false;
  ws.isAlive            = true;

  ws.on('message', async (data, isBinary) => {
    try {
      if (!isBinary) {
        const text = data.toString('utf8');
        let msg;
        try { msg = JSON.parse(text); } catch { return; }

        if (msg?.type === 'hello' && msg?.deviceId) {
          ws.deviceId = msg.deviceId;
          console.log(`Camera ingest: device identified as ${ws.deviceId}`);
          await startSessionOnce(ws);
        }
        return;
      }

      // Binary frame — must have deviceId from hello handshake first
      if (!ws.deviceId) return;

      const frameBuffer = Buffer.isBuffer(data) ? data : Buffer.from(data);
      streamStore.setLatestFrame(ws.deviceId, frameBuffer);
      await startSessionOnce(ws);
    } catch (err) {
      console.error('[ws] camera message handling error:', err);
    }
  });

  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('error', (err) => console.error('Camera WS error:', err));
  ws.on('close', () =>
    console.log('Camera ingest disconnected', ws.deviceId || '(unidentified)')
  );
});

/* ── /audio-ingest — Flutter mic → server ────────────────────────────────── */

const wssAudioIngest = new WebSocketServer({ noServer: true });

wssAudioIngest.on('connection', (ws) => {
  console.log('Audio ingest connected (Flutter mic active)');
  audioStore.clearChunks();

  debugChunks    = [];
  debugCapturing = true;

  let srcRate   = null;
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
          if (client.readyState === client.OPEN) client.send('stop', { binary: false });
        });

        if (debugCapturing && debugChunks.length > 0) {
          try {
            const combined = Buffer.concat(debugChunks);
            writeWav(combined, TARGET_RATE, path.join('/tmp', 'debug_audio.wav'));
          } catch (e) {
            console.error('Failed to write debug WAV:', e.message);
          }
          debugChunks    = [];
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

    const first4 = Array.from(rawBuf.slice(0, 4))
      .map(b => '0x' + b.toString(16).padStart(2, '0'));
    console.log(`Audio chunk: ${rawBuf.length} bytes @ ${srcRate} Hz | first bytes: ${first4.join(' ')}`);

    const outBuf = processAudio(resamplePCM(rawBuf, srcRate));
    console.log(`  → resampled to ${outBuf.length} bytes @ ${TARGET_RATE} Hz`);

    // Capture raw (pre-processAudio) so we hear actual mic input in debug WAV
    if (debugCapturing) debugChunks.push(Buffer.from(rawBuf));

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

    if (debugCapturing && debugChunks.length > 0) {
      try {
        const combined = Buffer.concat(debugChunks);
        writeWav(combined, TARGET_RATE, path.join('/tmp', 'debug_audio.wav'));
      } catch (e) {
        console.error('Failed to write debug WAV:', e.message);
      }
      debugChunks    = [];
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
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('error', (err) => console.error('Audio stream WS error:', err));
  ws.on('close', () => console.log('ESP32 audio stream disconnected'));
});

/* ── Upgrade router ──────────────────────────────────────────────────────── */

server.on('upgrade', (request, socket, head) => {
  const { pathname } = new URL(request.url, `http://${request.headers.host}`);

  if (pathname === '/ingest') {
    wssCamera.handleUpgrade(request, socket, head,
      (ws) => wssCamera.emit('connection', ws, request));
  } else if (pathname === '/audio-ingest') {
    wssAudioIngest.handleUpgrade(request, socket, head,
      (ws) => wssAudioIngest.emit('connection', ws, request));
  } else if (pathname === '/audio-stream') {
    wssAudioStream.handleUpgrade(request, socket, head,
      (ws) => wssAudioStream.emit('connection', ws, request));
  } else {
    socket.destroy();
  }
});

/* ── Heartbeat ───────────────────────────────────────────────────────────── */

function heartbeatAll(wss) {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) { ws.terminate(); return; }
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

server.listen(PORT, () => console.log(`${pkg.name}: listening on port ${PORT}`));

process.on('SIGTERM', () => {
  console.log(`${pkg.name}: received SIGTERM`);
  try { wssCamera.close(); }      catch (e) {}
  try { wssAudioIngest.close(); } catch (e) {}
  try { wssAudioStream.close(); } catch (e) {}
  server.close(() => process.exit(0));
});

/* ── Session auto-start helper ───────────────────────────────────────────── */

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
      successSeconds: parseFloat(process.env.SIT_SUCCESS_SECONDS  || '5'),
      timeoutSeconds: parseFloat(process.env.SESSION_TIMEOUT_SECONDS || '180'),
    });
    ws.autoSessionStarted = true;
  } catch (err) {
    console.error('[session] auto-start failed:', err);
  }
}

module.exports = server;