const streamStore = require('./streamStore');
const { predictFrame } = require('./mlClient');
const { writeDispenseCommand, writeFeedEvent, writeMlStatus } = require('./firebaseActions');

const POLL_INTERVAL_MS = parseInt(process.env.ML_POLL_INTERVAL_MS || '250', 10);
const SUCCESS_SECONDS = parseFloat(process.env.SIT_SUCCESS_SECONDS || '5');
const TIMEOUT_SECONDS = parseFloat(process.env.SESSION_TIMEOUT_SECONDS || '180');
const DISPENSE_GRAMS = parseInt(process.env.DISPENSE_GRAMS || '25', 10);

const sessions = new Map();

function getSession(deviceId) {
  return sessions.get(deviceId) || null;
}

function getPublicSession(deviceId) {
  const s = sessions.get(deviceId);
  if (!s) return null;
  return {
    deviceId,
    active: s.active,
    startedAt: s.startedAt,
    lastFrameId: s.lastFrameId,
    lastInferenceAt: s.lastInferenceAt,
    sitStartedAt: s.sitStartedAt,
    lastResult: s.lastResult,
    finalEvent: s.finalEvent,
    timeoutSeconds: s.timeoutSeconds,
    successSeconds: s.successSeconds,
  };
}

function listSessions() {
  return Array.from(sessions.keys()).map((deviceId) => getPublicSession(deviceId));
}

async function startSessionIfNeeded(deviceId) {
  if (!deviceId || sessions.has(deviceId)) return;

  const session = {
    deviceId,
    active: true,
    startedAt: Date.now(),
    sitStartedAt: null,
    lastFrameId: 0,
    lastInferenceAt: 0,
    running: false,
    finalEvent: null,
    lastResult: null,
    timeoutSeconds: TIMEOUT_SECONDS,
    successSeconds: SUCCESS_SECONDS,
    grams: DISPENSE_GRAMS,
  };

  sessions.set(deviceId, session);
  console.log(`[session] started ${deviceId}`);

  await writeFeedEvent(deviceId, {
    type: 'session_started',
    source: 'websocket-backend',
  });
  await writeMlStatus(deviceId, {
    status: 'running',
    event: { type: 'session_started' },
    session_started_at: session.startedAt,
    updatedAt: Date.now(),
  });

  runLoop(deviceId).catch((err) => {
    console.error(`[session] loop crashed for ${deviceId}:`, err);
    finishSession(deviceId, {
      type: 'session_error',
      error: String(err?.message || err),
    }).catch((inner) => console.error('finishSession error:', inner));
  });
}

async function runLoop(deviceId) {
  const session = sessions.get(deviceId);
  if (!session || session.running) return;
  session.running = true;

  while (session.active) {
    try {
      const now = Date.now();
      const elapsedSec = (now - session.startedAt) / 1000;

      if (elapsedSec >= session.timeoutSeconds) {
        await finishSession(deviceId, {
          type: 'sit_timeout',
          session_duration_sec: elapsedSec,
        });
        break;
      }

      const { latestJpeg, frameId } = streamStore.getLatestFrame(deviceId);
      if (!latestJpeg || !frameId || frameId === session.lastFrameId) {
        await sleep(POLL_INTERVAL_MS);
        continue;
      }

      session.lastFrameId = frameId;
      const ml = await predictFrame(latestJpeg);
      if (!ml) {
        await sleep(POLL_INTERVAL_MS);
        continue;
      }
      session.lastInferenceAt = Date.now();

      const posture = ml.posture || 'unknown';
      const dogDetected = !!ml.dog_detected;

      if (dogDetected && posture === 'sitting') {
        if (!session.sitStartedAt) session.sitStartedAt = session.lastInferenceAt;
      } else {
        session.sitStartedAt = null;
      }

      const sitDurationSec = session.sitStartedAt
        ? (session.lastInferenceAt - session.sitStartedAt) / 1000
        : 0;

      const payload = {
        ...ml,
        dog_detected: dogDetected,
        posture,
        sit_duration_sec: sitDurationSec,
        session_duration_sec: elapsedSec,
        frame_id: frameId,
        updatedAt: session.lastInferenceAt,
        status: 'running',
        event: null,
      };

      if (dogDetected && posture === 'sitting' && sitDurationSec >= session.successSeconds) {
        payload.event = { type: 'sit_success' };
      }

      session.lastResult = payload;
      await writeMlStatus(deviceId, payload);

      if (payload.event?.type === 'sit_success') {
        await finishSession(deviceId, {
          type: 'sit_success',
          session_duration_sec: elapsedSec,
          sit_duration_sec: sitDurationSec,
          sit_probability: payload.sit_probability,
          confidence: payload.confidence,
        });
        break;
      }
    } catch (err) {
      console.error(`[session] inference error for ${deviceId}:`, err);
      await writeMlStatus(deviceId, {
        status: 'error',
        event: { type: 'inference_error', message: String(err?.message || err) },
        updatedAt: Date.now(),
      });
      await sleep(POLL_INTERVAL_MS);
    }

    await sleep(POLL_INTERVAL_MS);
  }

  session.running = false;
}

async function finishSession(deviceId, event) {
  const session = sessions.get(deviceId);
  if (!session || !session.active) return;

  session.active = false;
  session.finalEvent = event;

  const finalPayload = {
    ...(session.lastResult || {}),
    status: 'finished',
    event,
    updatedAt: Date.now(),
  };

  await writeMlStatus(deviceId, finalPayload);
  await writeFeedEvent(deviceId, {
    ...event,
    source: 'websocket-backend',
  });

  if (event.type === 'sit_success') {
    await writeDispenseCommand(deviceId, session.grams);
  }

  console.log(`[session] finished ${deviceId}: ${event.type}`);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

module.exports = {
  startSessionIfNeeded,
  getSession,
  getPublicSession,
  listSessions,
};
