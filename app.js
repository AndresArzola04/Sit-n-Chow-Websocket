

const express = require('express');
const path = require('path');

const streamStore = require('./streamStore');

const app = express();

// Health check
app.get('/', (_req, res) => {
  res.status(200).send('OK');
});

// Optional debug viewer in browser
// Visit: https://YOUR_URL/view
// Quick test with test.jpg: node .\sendTestFrames.js .\test.jpg https://sit-n-chow-ws-96817124249.us-central1.run.app/ingest 5
app.get('/view', (_req, res) => {
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
    Pragma: 'no-cache',
    Connection: 'keep-alive',
  });

  let closed = false;
  req.on('close', () => {
    closed = true;
  });

  const fps = 15; // tune this
  const intervalMs = Math.floor(1000 / fps);

  const timer = setInterval(() => {
    if (closed) {
      clearInterval(timer);
      return;
    }

    const {latestJpeg, latestTs} = streamStore.getLatestFrame();
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