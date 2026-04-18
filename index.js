const express = require('express');
const fetch = require('node-fetch');
const http = require('http');
const { WebSocketServer } = require('ws');
const url = require('url');

const { initFirebase } = require('./firebaseActions');
const { setLatestChunk, clearChunks } = require('./audioStore');
const { startSession, stopSession, getPublicSession, listSessions } = require('./sessionManager');
const streamStore = require('./streamStore');
const baseApp = require('./app');

const app = express();
app.use(express.json({ limit: '10mb' }));

const firebase = initFirebase();
const admin = firebase?.admin || null;
const db = firebase?.db || null;

// ESP audio listeners keyed by deviceId
const audioClientsByDevice = new Map(); // deviceId -> Set<ws>

// ESP camera websocket clients keyed by deviceId
const cameraClientsByDevice = new Map(); // deviceId -> ws

function addAudioClient(deviceId, ws) {
  if (!deviceId) return;
  let set = audioClientsByDevice.get(deviceId);
  if (!set) {
    set = new Set();
    audioClientsByDevice.set(deviceId, set);
  }
  set.add(ws);
}

function removeAudioClient(deviceId, ws) {
  if (!deviceId) return;
  const set = audioClientsByDevice.get(deviceId);
  if (!set) return;
  set.delete(ws);
  if (set.size === 0) {
    audioClientsByDevice.delete(deviceId);
  }
}

function broadcastAudioChunk(deviceId, chunk) {
  const set = audioClientsByDevice.get(deviceId);
  if (!set || !set.size) return 0;

  let sent = 0;
  for (const ws of set) {
    if (ws.readyState !== ws.OPEN) continue;
    try {
      ws.send(chunk, { binary: true });
      sent++;
    } catch (err) {
      console.error('[audio-stream] send error:', err.message);
    }
  }
  return sent;
}

function sendAudioStop(deviceId) {
  const set = audioClientsByDevice.get(deviceId);
  if (!set || !set.size) return 0;

  let sent = 0;
  for (const ws of set) {
    if (ws.readyState !== ws.OPEN) continue;
    try {
      ws.send('stop');
      sent++;
    } catch (err) {
      console.error('[audio-stop] send error:', err.message);
    }
  }
  return sent;
}

function setCameraClient(deviceId, ws) {
  if (!deviceId) return;
  cameraClientsByDevice.set(deviceId, ws);
}

function removeCameraClient(deviceId, ws) {
  if (!deviceId) return;
  const existing = cameraClientsByDevice.get(deviceId);
  if (existing === ws) {
    cameraClientsByDevice.delete(deviceId);
  }
}

app.get('/healthz', (_req, res) => {
  res.json({
    ok: true,
    firebase: !!db,
    mlServiceUrl: process.env.ML_SERVICE_URL || null,
    audioDevicesConnected: Array.from(audioClientsByDevice.keys()),
    cameraDevicesConnected: Array.from(cameraClientsByDevice.keys()),
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

// Optional HTTP ingest support
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

// Mobile app uploads raw PCM here over HTTPS POST
app.post('/audio-ingest', express.raw({ type: '*/*', limit: '10mb' }), (req, res) => {
  try {
    const deviceId =
      req.query.deviceId ||
      req.headers['x-device-id'] ||
      req.headers['x-device'] ||
      '';

    if (!deviceId) {
      return res.status(400).json({ error: 'deviceId is required' });
    }

    if (!req.body || !req.body.length) {
      return res.status(400).json({ error: 'audio body required' });
    }

    const chunk = Buffer.from(req.body);
    setLatestChunk(chunk);

    const sent = broadcastAudioChunk(deviceId, chunk);
    return res.json({
      ok: true,
      bytes: req.body.length,
      deviceId,
      listeners: sent,
    });
  } catch (err) {
    console.error('[audio-ingest] error:', err);
    return res.status(500).json({ error: 'audio-ingest failed' });
  }
});

app.post('/audio-stop', express.json(), (req, res) => {
  try {
    const deviceId =
      req.query.deviceId ||
      req.body?.deviceId ||
      req.headers['x-device-id'] ||
      req.headers['x-device'] ||
      '';

    if (!deviceId) {
      return res.status(400).json({ error: 'deviceId is required' });
    }

    const sent = sendAudioStop(deviceId);
    clearChunks();

    return res.json({
      ok: true,
      deviceId,
      listeners: sent,
    });
  } catch (err) {
    console.error('[audio-stop] error:', err);
    return res.status(500).json({ error: 'audio-stop failed' });
  }
});

app.use(baseApp);

const server = http.createServer(app);

const audioWss = new WebSocketServer({ noServer: true });
const ingestWss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const parsed = url.parse(req.url, true);

  if (parsed.pathname === '/audio-stream') {
    const deviceId = parsed.query.device;
    if (!deviceId || typeof deviceId !== 'string') {
      socket.destroy();
      return;
    }

    audioWss.handleUpgrade(req, socket, head, (ws) => {
      ws.deviceId = deviceId;
      addAudioClient(deviceId, ws);

      console.log(`[audio-stream] ESP connected for device ${deviceId}`);

      ws.on('close', () => {
        removeAudioClient(deviceId, ws);
        console.log(`[audio-stream] ESP disconnected for device ${deviceId}`);
      });

      ws.on('error', (err) => {
        console.error(`[audio-stream] WS error for ${deviceId}:`, err.message);
        removeAudioClient(deviceId, ws);
      });

      try {
        ws.send(JSON.stringify({ ok: true, type: 'connected', deviceId }));
      } catch (_) {}
    });
    return;
  }

  if (parsed.pathname === '/ingest') {
    ingestWss.handleUpgrade(req, socket, head, (ws) => {
      let deviceId = null;

      console.log('[ingest] camera websocket connected');

      ws.on('message', async (data, isBinary) => {
        try {
          if (!isBinary) {
            const text = data.toString();

            try {
              const msg = JSON.parse(text);
              if (msg.type === 'hello' && msg.deviceId) {
                deviceId = msg.deviceId;
                setCameraClient(deviceId, ws);

                console.log(`[ingest] hello from device ${deviceId}`);

                await startSession(deviceId, {
                  successSeconds: 5,
                  timeoutSeconds: 180,
                });
              }
            } catch (_) {
              // ignore non-JSON text messages
            }

            return;
          }

          if (!deviceId) {
            console.warn('[ingest] binary frame received before hello/deviceId');
            return;
          }

          const frameBuffer = Buffer.from(data);
          streamStore.setLatestFrame(deviceId, frameBuffer);

          await startSession(deviceId, {
            successSeconds: 5,
            timeoutSeconds: 180,
          });
        } catch (err) {
          console.error('[ingest] websocket message error:', err.message);
        }
      });

      ws.on('close', () => {
        if (deviceId) {
          removeCameraClient(deviceId, ws);
          console.log(`[ingest] camera websocket disconnected for ${deviceId}`);
        } else {
          console.log('[ingest] camera websocket disconnected');
        }
      });

      ws.on('error', (err) => {
        console.error('[ingest] camera websocket error:', err.message);
        if (deviceId) {
          removeCameraClient(deviceId, ws);
        }
      });
    });
    return;
  }

  socket.destroy();
});

const port = Number(process.env.PORT || 8080);
server.listen(port, () => {
  console.log(`websockets: listening on port ${port}`);
});