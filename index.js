const express = require('express');
const fetch = require('node-fetch');

const {
  initFirebase,
} = require('./firebaseActions');
const { setLatestChunk } = require('./audioStore');
const { startSession, stopSession, getPublicSession, listSessions } = require('./sessionManager');
const streamStore = require('./streamStore');
const baseApp = require('./app');

const app = express();
app.use(express.json({ limit: '10mb' }));

const firebase = initFirebase();
const admin = firebase?.admin || null;
const db = firebase?.db || null;

app.get('/healthz', (_req, res) => {
  res.json({
    ok: true,
    firebase: !!db,
    mlServiceUrl: process.env.ML_SERVICE_URL || null,
  });
});

app.get('/esp-token', async (req, res) => {
  if (!admin || !db) {
    return res.status(503).json({ error: 'Firebase not initialised' });
  }

  const deviceId = req.query.device;
  if (!deviceId || typeof deviceId !== 'string' || deviceId.length > 64) {
    return res.status(400).json({ error: 'Missing or invalid device param' });
  }

  const secret = process.env.ESP_DEVICE_SECRET;
  if (secret && req.headers['x-device-secret'] !== secret) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    const customToken = await admin.auth().createCustomToken(deviceId, {
      deviceId,
      role: 'device',
    });

    const apiKey = process.env.FIREBASE_API_KEY;
    if (!apiKey) {
      return res.status(500).json({ error: 'Server misconfigured: FIREBASE_API_KEY missing' });
    }

    const authRes = await fetch(
      `https://identitytoolkit.googleapis.com/v1/accounts:signInWithCustomToken?key=${apiKey}`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          token: customToken,
          returnSecureToken: true,
        }),
      }
    );

    const authData = await authRes.json();

    if (!authRes.ok || !authData.idToken) {
      console.error('[esp-token] ID token exchange failed:', authData);
      return res.status(500).json({ error: 'Token exchange failed' });
    }

    return res.json({
      ok: true,
      token: authData.idToken,
      deviceId,
      expiresIn: Number(authData.expiresIn || 3600),
    });
  } catch (err) {
    console.error('[esp-token] error:', err);
    return res.status(500).json({ error: 'Token generation failed' });
  }
});

app.post('/ingest', async (req, res) => {
  try {
    const { deviceId, imageBase64 } = req.body || {};
    if (!deviceId || !imageBase64) {
      return res.status(400).json({ error: 'deviceId and imageBase64 are required' });
    }

    const frameBuffer = Buffer.from(imageBase64, 'base64');
    streamStore.setLatestFrame(deviceId, frameBuffer);

    await startSession(deviceId, {
      successSeconds: 5,
      timeoutSeconds: 180,
    });

    return res.json({ ok: true });
  } catch (err) {
    console.error('[ingest] error:', err);
    return res.status(500).json({ error: 'ingest failed' });
  }
});

app.post('/sessions/:deviceId/start', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const successSeconds = Number(req.query.success_seconds || req.body?.successSeconds || 5);
    const timeoutSeconds = Number(req.query.timeout_seconds || req.body?.timeoutSeconds || 180);

    await startSession(deviceId, { successSeconds, timeoutSeconds });
    return res.json({ ok: true, session: getPublicSession(deviceId) });
  } catch (err) {
    console.error('[session start] error:', err);
    return res.status(500).json({ error: 'session start failed' });
  }
});

app.post('/sessions/:deviceId/stop', async (req, res) => {
  try {
    const { deviceId } = req.params;
    await stopSession(deviceId, { type: 'stopped' });
    return res.json({ ok: true, session: getPublicSession(deviceId) });
  } catch (err) {
    console.error('[session stop] error:', err);
    return res.status(500).json({ error: 'session stop failed' });
  }
});

app.get('/sessions/:deviceId', (req, res) => {
  const { deviceId } = req.params;
  return res.json(getPublicSession(deviceId) || null);
});

app.get('/sessions', (_req, res) => {
  return res.json(listSessions());
});

app.post('/audio-ingest', express.raw({ type: '*/*', limit: '10mb' }), (req, res) => {
  try {
    if (!req.body || !req.body.length) {
      return res.status(400).json({ error: 'audio body required' });
    }

    setLatestChunk(Buffer.from(req.body));
    return res.json({ ok: true, bytes: req.body.length });
  } catch (err) {
    console.error('[audio-ingest] error:', err);
    return res.status(500).json({ error: 'audio-ingest failed' });
  }
});

app.use(baseApp);

const port = Number(process.env.PORT || 8080);
app.listen(port, () => {
  console.log(`websockets: listening on port ${port}`);
});