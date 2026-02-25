const fs = require('fs');
const WebSocket = require('ws');

const JPEG_PATH = process.argv[2] || './test.jpg';
const WS_URL = process.argv[3] || 'ws://localhost:8080/ingest';
const FPS = Number(process.argv[4] || 5);

const jpeg = fs.readFileSync(JPEG_PATH);

const ws = new WebSocket(WS_URL);

ws.on('open', () => {
  console.log('Connected. Sending frames:', JPEG_PATH, '->', WS_URL);

  const intervalMs = Math.floor(1000 / FPS);
  setInterval(() => {
    ws.send(jpeg); // binary JPEG bytes
  }, intervalMs);
});

ws.on('error', (e) => console.error('WS error:', e));
ws.on('close', () => console.log('Closed'));