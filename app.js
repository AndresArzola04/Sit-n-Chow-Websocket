const express = require('express');
const path = require('path');

const streamStore = require('./streamStore');

const app = express();

// CORS middleware - allows Flutter mobile/web to access all endpoints
app.use((_req, res, next) => {
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.set('Access-Control-Allow-Headers', '*');
  next();
});

// Handle OPTIONS preflight
app.options('*', (_req, res) => {
  res.sendStatus(204);
});

// Health check
app.get('/', (_req, res) => {
  res.status(200).send('OK');
});

// Serves the latest JPEG frame as a raw image (for Flutter app polling)
app.get('/view', (_req, res) => {
  const { latestJpeg, latestTs } = streamStore.getLatestFrame();

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

// Browser debug viewer
app.get('/debug', (_req, res) => {
  res
    .status(200)
    .send(`<!doctype html>
<html>
  <head><meta charset="utf-8"><title>MJPEG Viewer</title></head>
  <body style="margin:0; background:#111; display:flex; align-items:center; justify-content:center; height:100vh;">
    <img src="/stream.mjpeg" style="max-width:100%; max-height:100%;" />
  </body>
</html>`);
});

// MJPEG stream endpoint
app.get('/stream.mjpeg', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'multipart/x-mixed-replace; boundary=frame',
    'Cache-Control': 'no-cache, no-store, must-revalidate',
    'Pragma': 'no-cache',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': '*',
  });

  let closed = false;
  req.on('close', () => {
    closed = true;
  });

  const fps = 15;
  const intervalMs = Math.floor(1000 / fps);

  const timer = setInterval(() => {
    if (closed) {
      clearInterval(timer);
      return;
    }

    const { latestJpeg, latestTs } = streamStore.getLatestFrame();
    if (!latestJpeg) return;

    res.write(`--frame\r\n`);
    res.write(`Content-Type: image/jpeg\r\n`);
    res.write(`Content-Length: ${latestJpeg.length}\r\n`);
    res.write(`X-Timestamp: ${latestTs}\r\n\r\n`);
    res.write(latestJpeg);
    res.write('\r\n');
  }, intervalMs);
});

module.exports = app;