/*
 * audioStore.js
 *
 * In-memory store for the latest audio chunk from the Flutter intercom.
 * Mirrors the streamStore pattern used for camera frames.
 *
 * The Flutter app pushes raw binary PCM chunks to /audio-ingest.
 * The ESP32 receives them via /audio-stream WebSocket.
 *
 * Only the latest chunk is kept — if the ESP falls behind, it skips
 * stale chunks rather than buffering them (real-time audio, not recording).
 */

let latestChunk    = null;   // Buffer of raw 16-bit PCM bytes
let latestTs       = 0;
let chunkId        = 0;      // Monotonically incrementing — ESP uses this to
                             // detect a new chunk, same role as frameId

function setLatestChunk(buf) {
  latestChunk = buf;
  latestTs    = Date.now();
  chunkId++;
}

function clearChunks() {
  latestChunk = null;
  latestTs    = 0;
  // Do NOT reset chunkId — the ESP uses it to detect the clear (null chunk)
  // vs. a new chunk. Resetting would cause a false "new chunk" on next connect.
}

function getLatestChunk() {
  return { latestChunk, latestTs, chunkId };
}

module.exports = { setLatestChunk, clearChunks, getLatestChunk };
