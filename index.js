/* eslint-disable no-process-exit */

const http    = require('http');
const { WebSocketServer } = require('ws');

const pkg         = require('./package');
const app         = require('./app');
const streamStore = require('./streamStore');
const audioStore  = require('./audioStore');

const PORT = parseInt(process.env.PORT, 10) || 8080;

/* ── Firebase Admin SDK ──────────────────────────────────────────────────── */

let admin = null;
let db    = null;

function initFirebaseAdmin() {
  try {
    admin = require('firebase-admin');
    const raw = process.env.FIREBASE_SERVICE_ACCOUNT;
    if (!raw) {
      console.warn('FIREBASE_SERVICE_ACCOUNT not set — Firebase features disabled');
      return;
    }
    const serviceAccount = JSON.parse(raw);
    admin.initializeApp({
      credential:  admin.credential.cert(serviceAccount),
      databaseURL: process.env.FIREBASE_DATABASE_URL,
    });
    db = admin.database();
    console.log('Firebase Admin SDK initialised');
  } catch (err) {
    console.error('Firebase Admin init failed:', err.message);
  }
}

initFirebaseAdmin();

/* ── PCM utilities ───────────────────────────────────────────────────────── */

const TARGET_RATE = 16000;

/*
 * Swap every pair of bytes in-place.
 * Used to convert big-endian int16 (CAFF/iOS default) → little-endian
 * which is what the ESP audio player expects.
 */
function swapBytes(buf) {
  const out = Buffer.allocUnsafe(buf.length);
  for (let i = 0; i + 1 < buf.length; i += 2) {
    out[i]     = buf[i + 1];
    out[i + 1] = buf[i];
  }
  return out;
}

/*
 * Linear interpolation resampler: srcRate → TARGET_RATE (16000 Hz).
 * Input must be little-endian int16 PCM.
 */
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
    const out = Math.round(s0 + frac * (s1 - s0));
    outputBuf.writeInt16LE(Math.max(-32768, Math.min(32767, out)), i * 2);
  }

  return outputBuf;
}

/* ── ESP32 token endpoint ────────────────────────────────────────────────── */

app.get('/esp-token', async (req, res) => {
  if (!admin || !db) return res.status(503).json({ error: 'Firebase not initialised' });

  const deviceId = req.query.device;
  if (!deviceId || typeof deviceId !== 'string' || deviceId.length > 64)
    return res.status(400).json({ error: 'Missing or invalid device param' });

  const secret = process.env.DEVICE_SECRET;
  if (secret && req.headers['x-device-secret'] !== secret)
    return res.status(401).json({ error: 'Unauthorized' });

  try {
    const snap = await db.ref(`devices/${deviceId}/ownerUid`).once('value');
    if (!snap.exists()) return res.status(404).json({ error: 'Device not registered' });

    const customToken = await admin.auth().createCustomToken(deviceId, { role: 'device' });
    const apiKey = process.env.FIREBASE_API_KEY;
    if (!apiKey) return res.status(500).json({ error: 'Server misconfigured' });

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
  if (!db || !admin) return;
  db.ref('notifications').on('child_added', async (uidSnap) => {
    const uid = uidSnap.key;
    uidSnap.ref.on('child_added', async (notifSnap) => {
      const notif = notifSnap.val();
      if (!notif || !notif.title) return;
      await notifSnap.ref.remove();
      const tokensSnap = await db.ref(`userFcmTokens/${uid}`).once('value');
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
  });
  console.log('Notification forwarder listening');
}

startNotificationForwarder();

/* ── HTTP + WebSocket server ─────────────────────────────────────────────── */

const server = http.createServer(app);

/* ── /ingest — ESP32 camera frames ──────────────────────────────────────── */

const wssCamera = new WebSocketServer({ noServer: true });
wssCamera.on('connection', (ws) => {
  console.log('Camera ingest connected');
  ws.on('message', (data, isBinary) => {
    if (!isBinary) return;
    streamStore.setLatestFrame(Buffer.from(data));
  });
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('error', (err) => console.error('Camera WS error:', err));
  ws.on('close', () => console.log('Camera ingest disconnected'));
});

/* ── /audio-ingest — Flutter mic → server ────────────────────────────────── *
 *
 * Protocol:
 *   1. Connect
 *   2. Text: "sampleRate:<N>"          e.g. "sampleRate:44100"
 *   3. Text: "bigEndian:true|false"    whether PCM bytes are big-endian
 *                                      (iOS CAFF = true, Android raw = false)
 *   4. Binary frames: raw int16 PCM (no container, no header)
 *   5. Text: "stop"
 * ─────────────────────────────────────────────────────────────────────────── */

const wssAudioIngest = new WebSocketServer({ noServer: true });

wssAudioIngest.on('connection', (ws) => {
  console.log('Audio ingest connected (Flutter mic active)');
  audioStore.clearChunks();

  let srcRate   = null;
  let bigEndian = false;  // default: little-endian (Android)

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

    // iOS CAFF stores PCM as big-endian — swap to little-endian for the ESP
    if (bigEndian) {
      rawBuf = swapBytes(rawBuf);
    }

    const first4 = Array.from(rawBuf.slice(0, 4))
      .map(b => '0x' + b.toString(16).padStart(2, '0'));
    console.log(`Audio chunk: ${rawBuf.length} bytes @ ${srcRate} Hz | first bytes: ${first4.join(' ')}`);

    const outBuf = resamplePCM(rawBuf, srcRate);
    console.log(`  → resampled to ${outBuf.length} bytes @ ${TARGET_RATE} Hz`);

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
  });

  ws.on('error', (err) => console.error('Audio ingest WS error:', err));
});

/* ── /audio-stream — server → ESP32 ─────────────────────────────────────── */

const wssAudioStream = new WebSocketServer({ noServer: true });
wssAudioStream.on('connection', (ws) => {
  console.log('ESP32 audio stream connected');
  const { latestChunk } = audioStore.getLatestChunk();
  if (latestChunk) ws.send(latestChunk, { binary: true });
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('error', (err) => console.error('Audio stream WS error:', err));
  ws.on('close', () => console.log('ESP32 audio stream disconnected'));
});

/* ── Upgrade router ──────────────────────────────────────────────────────── */

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
  try { wssCamera.close(); } catch (e) {}
  try { wssAudioIngest.close(); } catch (e) {}
  try { wssAudioStream.close(); } catch (e) {}
  server.close(() => process.exit(0));
});

module.exports = server;