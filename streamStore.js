let latestJpeg = null;
let latestTs = 0;
let frameId = 0;

function setLatestFrame(buf) {
  latestJpeg = buf;
  latestTs = Date.now();
  frameId++;
}

function getLatestFrame() {
  return { latestJpeg, latestTs, frameId };
}

module.exports = { setLatestFrame, getLatestFrame };