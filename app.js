const express = require('express');
const fs      = require('fs');
const path    = require('path');

const streamStore = require('./streamStore');
const { startSession, stopSession, getPublicSession, listSessions } = require('./sessionManager');

const app = express();

app.use(express.json());

// CORS — allows Flutter mobile/web to access all endpoints
app.use((_req, res, next) => {
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.set('Access-Control-Allow-Headers', '*');
  next();
});

app.options('*', (_req, res) => res.sendStatus(204));

// ── Health checks ──────────────────────────────────────────────────────────

app.get('/', (_req, res) => res.status(200).send('OK'));

app.get('/health', (_req, res) => res.json({ status: 'ok' }));

// ── Camera frame endpoints ─────────────────────────────────────────────────

// Serves the latest JPEG frame as a raw image (for Flutter app polling).
// Requires ?device=<deviceId>
app.get('/view', (req, res) => {
  const deviceId = req.query.device;

  if (!deviceId) {
    return res.status(400).send('Missing device query param');
  }

  const { latestJpeg, latestTs } = streamStore.getLatestFrame(deviceId);

  if (!latestJpeg) {
    return res.status(503).send('No frame available yet');
  }

  res.writeHead(200, {
    'Content-Type':   'image/jpeg',
    'Content-Length': latestJpeg.length,
    'Cache-Control':  'no-cache, no-store, must-revalidate',
    Pragma:           'no-cache',
    'X-Timestamp':    latestTs,
  });
  res.end(latestJpeg);
});

// Browser debug viewer — pass ?device=<deviceId> in URL
app.get('/debug', (req, res) => {
  const deviceId = req.query.device || '';

  res.status(200).send(`<!doctype html>
<html>
  <head><meta charset="utf-8"><title>MJPEG Viewer</title></head>
  <body style="margin:0; background:#111; display:flex; align-items:center; justify-content:center; height:100vh;">
    <img src="/stream.mjpeg?device=${encodeURIComponent(deviceId)}" style="max-width:100%; max-height:100%;" />
  </body>
</html>`);
});

// MJPEG stream endpoint — requires ?device=<deviceId>
app.get('/stream.mjpeg', (req, res) => {
  const deviceId = req.query.device;

  if (!deviceId) {
    return res.status(400).send('Missing device query param');
  }

  res.writeHead(200, {
    'Content-Type':               'multipart/x-mixed-replace; boundary=frame',
    'Cache-Control':              'no-cache, no-store, must-revalidate',
    Pragma:                       'no-cache',
    Connection:                   'keep-alive',
    'Access-Control-Allow-Origin': '*',
  });

  let closed      = false;
  let lastFrameId = -1;

  req.on('close', () => { closed = true; });

  const intervalMs    = Math.floor(1000 / 15);
  const BOUNDARY      = '--frame\r\n';
  const CONTENT_TYPE  = 'Content-Type: image/jpeg\r\n';
  const CRLF          = '\r\n';

  const timer = setInterval(() => {
    if (closed) { clearInterval(timer); return; }

    const { latestJpeg, latestTs, frameId } = streamStore.getLatestFrame(deviceId);
    if (!latestJpeg || frameId === lastFrameId) return;

    lastFrameId = frameId;
    res.write(BOUNDARY);
    res.write(CONTENT_TYPE);
    res.write(`Content-Length: ${latestJpeg.length}\r\nX-Timestamp: ${latestTs}\r\n\r\n`);
    res.write(latestJpeg);
    res.write(CRLF);
  }, intervalMs);
});

// ── ML Session management routes ───────────────────────────────────────────
//
// POST /sessions/:deviceId/start  — start (or resume) an ML inference session
// POST /sessions/:deviceId/stop   — manually stop a running session
// GET  /session?device=<id>       — get session state for one device
// GET  /sessions                  — list all active/recent sessions

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

    const session = await startSession(deviceId, { successSeconds, timeoutSeconds, grams });

    return res.json({ ok: true, message: 'Session started', session });
  } catch (err) {
    return res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.post('/sessions/:deviceId/stop', async (req, res) => {
  try {
    const { deviceId } = req.params;
    await stopSession(deviceId, { type: 'session_stopped', reason: 'manual_stop' });

    return res.json({ ok: true, message: 'Session stopped', deviceId });
  } catch (err) {
    return res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

app.get('/session', (req, res) => {
  const deviceId = req.query.device;
  if (!deviceId) return res.status(400).json({ error: 'Missing device query param' });

  const session = getPublicSession(deviceId);
  if (!session) return res.status(404).json({ error: 'No active session for device' });

  res.json(session);
});

app.get('/sessions', (_req, res) => {
  res.json({ sessions: listSessions() });
});

// ── Audio debug capture ────────────────────────────────────────────────────
//
// After making a call, visit /debug-audio to download the WAV that was sent
// to the ESP32. Useful for isolating audio issues before vs after the server.

app.get('/debug-audio', (_req, res) => {
  const filePath = path.join('/tmp', 'debug_audio.wav');

  if (!fs.existsSync(filePath)) {
    return res.status(404).send(
      'No audio captured yet. Make a call first, then visit this URL after hanging up.'
    );
  }

  res.setHeader('Content-Type', 'audio/wav');
  res.setHeader('Content-Disposition', 'attachment; filename="debug_audio.wav"');
  fs.createReadStream(filePath).pipe(res);
});

// ── Helpers ────────────────────────────────────────────────────────────────

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

module.exports = app;