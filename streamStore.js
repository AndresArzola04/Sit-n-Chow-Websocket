/*
 * streamStore.js
 *
 * In-memory store for the latest JPEG frame from each device.
 * Keyed by deviceId so multiple ESP32 cameras can stream concurrently.
 *
 * The ESP32 sends frames over the /ingest WebSocket. The MJPEG stream
 * (/stream.mjpeg) and snapshot (/view) endpoints read from here.
 *
 * Only the latest frame per device is kept — viewers that fall behind
 * skip stale frames rather than buffering them (real-time display).
 */

const frames = new Map();

/**
 * Store the latest JPEG buffer for a device.
 * @param {string} deviceId
 * @param {Buffer} buf  Raw JPEG bytes
 * @returns {number} The new frameId
 */
function setLatestFrame(deviceId, buf) {
  if (!deviceId) return 0;

  const existing   = frames.get(deviceId);
  const nextFrameId = existing ? existing.frameId + 1 : 1;

  frames.set(deviceId, {
    latestJpeg: buf,
    latestTs:   Date.now(),
    frameId:    nextFrameId,
  });

  return nextFrameId;
}

/**
 * Retrieve the latest frame for a device.
 * Returns a zero-value object if no frame has been received yet.
 * @param {string} deviceId
 * @returns {{ latestJpeg: Buffer|null, latestTs: number, frameId: number }}
 */
function getLatestFrame(deviceId) {
  if (!deviceId) {
    return { latestJpeg: null, latestTs: 0, frameId: 0 };
  }

  return frames.get(deviceId) || { latestJpeg: null, latestTs: 0, frameId: 0 };
}

module.exports = { setLatestFrame, getLatestFrame };