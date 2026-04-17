const express = require('express');
const { Readable } = require('stream');
const fetch = require('node-fetch');

const {
  initFirebase,
  writeFeedEvent,
  writeDeviceMlResult,
} = require('./firebaseActions');
const { createApp } = require('./app');
const { attachDebugAudioRoute, setAudioChunk } = require('./audioStore');
const {
  ingestFrame,
  getLatestFrameBuffer,
  attachViewer,
  clearDevice,
} = require('./streamStore');
const {
  ensureSession,
  startSession,
  stopSession,
  getSessionState,
  getAllSessionsState,
  setSessionMlResult,
  attachSessionManager,
} = require('./sessionManager');

const app = express();
app.use(express.json({ limit: '2mb' }));

const firebase = initFirebase();
const admin = firebase?.admin || null;
const db = firebase?.db || null;

attachSessionManager({
  onFinal: async ({ deviceId, finalEvent, lastResult }) => {
    console.log(`[session] final for ${deviceId}:`, finalEvent);
    if (db) {
      try {
        await writeFeedEvent(db, deviceId, finalEvent);
      } catch (err) {
        console.error('[firebase] writeFeedEvent failed:', err.message);
      }
      try {
        await writeDeviceMlResult(db, deviceId, lastResult, finalEvent);
      } catch (err) {
        console.error('[firebase] writeDeviceMlResult failed:', err.message);
      }
    }
  },
});

app.get('/health', (_req, res) => {
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
    ingestFrame(deviceId, frameBuffer);

    ensureSession(deviceId);
    await startSession(deviceId, {
      successSeconds: 5,
      timeoutSeconds: 180,
    });

    res.json({ ok: true });
  } catch (err) {
    console.error('[ingest] error:', err);
    res.status(500).json({ error: 'ingest failed' });
  }
});

app.get('/stream.mjpeg', (req, res) => {
  const deviceId = req.query.deviceId;
  if (!deviceId) {
    return res.status(400).send('deviceId query param required');
  }

  res.writeHead(200, {
    'Cache-Control': 'no-cache',
    'Pragma': 'no-cache',
    'Connection': 'close',
    'Content-Type': 'multipart/x-mixed-replace; boundary=frame',
  });

  const detach = attachViewer(deviceId, (frameBuffer) => {
    try {
      res.write(`--frame\r\nContent-Type: image/jpeg\r\nContent-Length: ${frameBuffer.length}\r\n\r\n`);
      res.write(frameBuffer);
      res.write('\r\n');
    } catch (e) {
      detach();
      try {
        res.end();
      } catch (_) {}
    }
  });

  const latest = getLatestFrameBuffer(deviceId);
  if (latest) {
    try {
      res.write(`--frame\r\nContent-Type: image/jpeg\r\nContent-Length: ${latest.length}\r\n\r\n`);
      res.write(latest);
      res.write('\r\n');
    } catch (_) {}
  }

  req.on('close', () => {
    detach();
  });
});

app.post('/sessions/:deviceId/start', async (req, res) => {
  try {
    const { deviceId } = req.params;
    const successSeconds = Number(req.query.success_seconds || req.body?.successSeconds || 5);
    const timeoutSeconds = Number(req.query.timeout_seconds || req.body?.timeoutSeconds || 180);

    await startSession(deviceId, { successSeconds, timeoutSeconds });
    res.json({ ok: true, session: getSessionState(deviceId) });
  } catch (err) {
    console.error('[session start] error:', err);
    res.status(500).json({ error: 'session start failed' });
  }
});

app.post('/sessions/:deviceId/stop', async (req, res) => {
  try {
    const { deviceId } = req.params;
    await stopSession(deviceId, { type: 'stopped' });
    res.json({ ok: true, session: getSessionState(deviceId) });
  } catch (err) {
    console.error('[session stop] error:', err);
    res.status(500).json({ error: 'session stop failed' });
  }
});

app.get('/sessions/:deviceId', (req, res) => {
  const { deviceId } = req.params;
  res.json(getSessionState(deviceId) || null);
});

app.get('/sessions', (_req, res) => {
  res.json(getAllSessionsState());
});

app.post('/ml-result', async (req, res) => {
  try {
    const { deviceId, result, finalEvent } = req.body || {};
    if (!deviceId || !result) {
      return res.status(400).json({ error: 'deviceId and result are required' });
    }

    setSessionMlResult(deviceId, result);

    if (db) {
      try {
        await writeDeviceMlResult(db, deviceId, result, finalEvent || null);
      } catch (err) {
        console.error('[firebase] writeDeviceMlResult failed:', err.message);
      }
    }

    if (finalEvent) {
      await stopSession(deviceId, finalEvent);
    }

    res.json({ ok: true });
  } catch (err) {
    console.error('[ml-result] error:', err);
    res.status(500).json({ error: 'ml-result failed' });
  }
});

app.post('/audio-stream', express.raw({ type: '*/*', limit: '10mb' }), (req, res) => {
  try {
    if (!req.body || !req.body.length) {
      return res.status(400).json({ error: 'audio body required' });
    }
    setAudioChunk(Buffer.from(req.body));
    res.json({ ok: true, bytes: req.body.length });
  } catch (err) {
    console.error('[audio-stream] error:', err);
    res.status(500).json({ error: 'audio-stream failed' });
  }
});

attachDebugAudioRoute(app);

app.use(createApp({
  getLatestFrameBuffer,
  onFrame: async ({ deviceId, frameBuffer }) => {
    ingestFrame(deviceId, frameBuffer);
    ensureSession(deviceId);
    await startSession(deviceId, {
      successSeconds: 5,
      timeoutSeconds: 180,
    });
  },
  onDisconnect: ({ deviceId }) => {
    clearDevice(deviceId);
  },
  getMlServiceUrl: () => process.env.ML_SERVICE_URL || null,
  onMlResult: async ({ deviceId, result, finalEvent }) => {
    setSessionMlResult(deviceId, result);

    if (db) {
      try {
        await writeDeviceMlResult(db, deviceId, result, finalEvent || null);
      } catch (err) {
        console.error('[firebase] writeDeviceMlResult failed:', err.message);
      }
      if (finalEvent) {
        try {
          await writeFeedEvent(db, deviceId, finalEvent);
        } catch (err) {
          console.error('[firebase] writeFeedEvent failed:', err.message);
        }
      }
    }

    if (finalEvent) {
      await stopSession(deviceId, finalEvent);
    }
  },
}));

const port = Number(process.env.PORT || 8080);
app.listen(port, () => {
  console.log(`websockets: listening on port ${port}`);
});