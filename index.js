/* eslint-disable no-process-exit */

const http    = require('http');
const { WebSocketServer } = require('ws');

const pkg         = require('./package');
const app         = require('./app');
const streamStore = require('./streamStore');

const PORT = parseInt(process.env.PORT, 10) || 8080;

/* ── Firebase Admin SDK ──────────────────────────────────────────────────── */
// Set the FIREBASE_SERVICE_ACCOUNT env var in Cloud Run to the full contents
// of your service account JSON key (single line, no newlines).
//
// Deploy command:
//   gcloud run services update sit-n-chow-ws \
//     --region us-central1 \
//     --set-env-vars "FIREBASE_SERVICE_ACCOUNT=$(cat key.json | tr -d '\n')"

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
      // e.g. "https://sit-n-chow-5eeae-default-rtdb.firebaseio.com"
    });

    db = admin.database();
    console.log('Firebase Admin SDK initialised');
  } catch (err) {
    console.error('Firebase Admin init failed:', err.message);
  }
}

initFirebaseAdmin();

/* ── ESP32 token endpoint ─────────────────────────────────────────────────
 *
 * The ESP32 calls GET /esp-token?device=<deviceId>
 *
 * We verify the device exists in Firebase, then mint a custom token for it.
 * Custom tokens are valid for 1 hour; the ESP32 refreshes every 55 minutes.
 *
 * The ESP32 then uses this token in Firebase REST calls as:
 *   ?auth=<customToken>
 *
 * Security: in production add a shared secret header so only your ESP32s
 * can call this endpoint (see CONFIG_CLOUD_RUN_DEVICE_SECRET in Kconfig).
 * ─────────────────────────────────────────────────────────────────────── */
app.get('/esp-token', async (req, res) => {
  if (!admin || !db) {
    return res.status(503).json({ error: 'Firebase not initialised' });
  }

  const deviceId = req.query.device;
  if (!deviceId || typeof deviceId !== 'string' || deviceId.length > 64) {
    return res.status(400).json({ error: 'Missing or invalid device param' });
  }

  // Optional shared-secret check — set DEVICE_SECRET env var + Kconfig value
  const secret = process.env.DEVICE_SECRET;
  if (secret) {
    const provided = req.headers['x-device-secret'];
    if (provided !== secret) {
      return res.status(401).json({ error: 'Unauthorized' });
    }
  }

  try {
    // Verify the device is registered (has an ownerUid)
    const snap = await db.ref(`devices/${deviceId}/ownerUid`).once('value');
    if (!snap.exists()) {
      return res.status(404).json({ error: 'Device not registered' });
    }

    // Step 1: Mint a custom token whose UID is the deviceId.
    const customToken = await admin.auth().createCustomToken(deviceId, {
      role: 'device',
    });

    // Step 2: Exchange custom token for an ID token via Firebase Auth REST API.
    // Custom tokens cannot be used as ?auth= in REST calls — only ID tokens work.
    const apiKey = process.env.FIREBASE_API_KEY;
    if (!apiKey) {
      console.error('FIREBASE_API_KEY not set');
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

    return res.json({
      token: authData.idToken,
      expiresIn: 3600,
    });
  } catch (err) {
    console.error('Token mint error:', err);
    return res.status(500).json({ error: 'Token generation failed' });
  }
});

/* ── Notification forwarder ───────────────────────────────────────────────
 *
 * Watches Firebase notifications/<uid> for new entries written by the ESP32
 * and forwards them to the user's FCM token(s).
 *
 * This replaces needing a separate Cloud Function.
 * ─────────────────────────────────────────────────────────────────────── */
function startNotificationForwarder() {
  if (!db || !admin) return;

  // Watch all notification queues
  db.ref('notifications').on('child_added', async (uidSnap) => {
    const uid = uidSnap.key;

    uidSnap.ref.on('child_added', async (notifSnap) => {
      const notif = notifSnap.val();
      if (!notif || !notif.title) return;

      // Delete immediately to avoid re-processing on server restart
      await notifSnap.ref.remove();

      // Fetch user's FCM tokens
      const tokensSnap = await db.ref(`userFcmTokens/${uid}`).once('value');
      if (!tokensSnap.exists()) return;

      const tokens = Object.keys(tokensSnap.val());
      if (tokens.length === 0) return;

      const message = {
        notification: {
          title: notif.title,
          body:  notif.body || '',
        },
        data: {
          deviceId: notif.deviceId || '',
          ts:       String(notif.ts || Date.now()),
        },
        tokens,
      };

      try {
        const response = await admin.messaging().sendEachForMulticast(message);
        console.log(`Notification sent to ${uid}: ${response.successCount} ok, ${response.failureCount} failed`);

        // Clean up invalid tokens
        response.responses.forEach((r, i) => {
          if (!r.success &&
              r.error &&
              (r.error.code === 'messaging/registration-token-not-registered' ||
               r.error.code === 'messaging/invalid-registration-token')) {
            db.ref(`userFcmTokens/${uid}/${tokens[i]}`).remove();
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

const wss = new WebSocketServer({ server, path: '/ingest' });

wss.on('connection', (ws) => {
  ws.deviceId = null;

  ws.on('message', (data, isBinary) => {
    if (!isBinary) {
      try {
        const msg = JSON.parse(data.toString());

        if (msg.type === 'hello' && typeof msg.deviceId === 'string' && msg.deviceId.length > 0) {
          ws.deviceId = msg.deviceId;
          console.log(`WS registered deviceId=${ws.deviceId}`);
        }
      } catch (err) {
        console.warn('Invalid WS text message:', err.message);
      }
      return;
    }

    if (!ws.deviceId) {
      console.warn('Dropping frame because deviceId was not set first');
      return;
    }

    streamStore.setLatestFrame(ws.deviceId, Buffer.from(data));
  });

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('error', (err) => console.error('WS error:', err));
});

// Heartbeat — kill dead connections
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