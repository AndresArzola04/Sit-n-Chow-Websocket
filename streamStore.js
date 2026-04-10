const frames = new Map();

function setLatestFrame(deviceId, buf) {
  if (!deviceId) return;

  const existing = frames.get(deviceId);
  const nextFrameId = existing ? existing.frameId + 1 : 1;

  frames.set(deviceId, {
    latestJpeg: buf,
    latestTs: Date.now(),
    frameId: nextFrameId,
  });
}

function getLatestFrame(deviceId) {
  if (!deviceId) {
    return { latestJpeg: null, latestTs: 0, frameId: 0 };
  }

  return frames.get(deviceId) || { latestJpeg: null, latestTs: 0, frameId: 0 };
}

module.exports = { setLatestFrame, getLatestFrame };