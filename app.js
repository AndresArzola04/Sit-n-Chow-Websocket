const express = require('express');
const fs = require('fs');
const path = require('path');

const streamStore = require('./streamStore');
const { getPublicSession, listSessions } = require('./sessionManager');

const app = express();

app.use((_req, res, next) => {
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.set('Access-Control-Allow-Headers', '*');
  next();
});

app.options('*', (_req, res) => {
  res.sendStatus(204);
});

app.get('/', (_req, res) => {
  res.status(200).send('OK');
});

app.get('/health', (_req, res) => {
  res.json({ status: 'ok' });
});

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
    'Content-Type': 'image/jpeg',
    'Content-Length': latestJpeg.length,
    'Cache-Control': 'no-cache, no-store, must-revalidate',
    'Pragma': 'no-cache',
    'X-Timestamp': latestTs,
  });
  res.end(latestJpeg);
});

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

app.get('/stream.mjpeg', (req, res) => {
  const deviceId = req.query.device;
  if (!deviceId) {
    return res.status(400).send('Missing device query param');
  }

  res.writeHead(200, {
    'Content-Type': 'multipart/x-mixed-replace; boundary=frame',
    'Cache-Control': 'no-cache, no-store, must-revalidate',
    'Pragma': 'no-cache',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': '*',
  });

  let closed = false;
  let lastFrameId = -1;
  req.on('close', () => {
    closed = true;
  });

  const intervalMs = Math.floor(1000 / 15);
  const BOUNDARY = '--frame\r\n';
  const CONTENT_TYPE = 'Content-Type: image/jpeg\r\n';
  const CRLF = '\r\n';

  const timer = setInterval(() => {
    if (closed) {
      clearInterval(timer);
      return;
    }

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

app.get('/session', (req, res) => {
  const deviceId = req.query.device;
  if (!deviceId) return res.status(400).json({ error: 'Missing device query param' });

  const session = getPublicSession(deviceId);
  if (!session) return res.status(404).json({ error: 'No active session for device' });

  return res.json(session);
});

app.get('/sessions', (_req, res) => {
  return res.json({ sessions: listSessions() });
});

app.get('/debug-audio', (_req, res) => {
  const filePath = path.join('/tmp', 'debug_audio.wav');
  if (!fs.existsSync(filePath)) {
    return res.status(404).send('No audio captured yet. Make a call first, then visit this URL after hanging up.');
  }
  res.setHeader('Content-Type', 'audio/wav');
  res.setHeader('Content-Disposition', 'attachment; filename="debug_audio.wav"');
  fs.createReadStream(filePath).pipe(res);
});

module.exports = app;
