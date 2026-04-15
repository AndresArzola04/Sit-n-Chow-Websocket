const sessions = new Map();

function startSession(deviceId, options = {}) {
  sessions.set(deviceId, {
    active: true,
    startedAt: Date.now(),
    sitStartedAt: null,
    sitProbabilities: [],
    lastInferenceAt: 0,
    dispensed: false,
    grams: options.grams ?? 25,
    source: options.source ?? 'schedule',
  });
}

function getSession(deviceId) {
  return sessions.get(deviceId) || null;
}

function ensureSession(deviceId, options = {}) {
  let session = sessions.get(deviceId);
  if (!session) {
    startSession(deviceId, options);
    session = sessions.get(deviceId);
  }
  return session;
}

function endSession(deviceId) {
  sessions.delete(deviceId);
}

module.exports = {
  startSession,
  getSession,
  ensureSession,
  endSession,
};