let latestJpeg = null;
let latestTs = 0;

function setLatestFrame(buf) {
  latestJpeg = buf;
  latestTs = Date.now();
}

function getLatestFrame() {
  return {latestJpeg, latestTs};
}

module.exports = {setLatestFrame, getLatestFrame};