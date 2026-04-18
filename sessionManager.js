const streamStore = require('./streamStore');
const { predictFrame } = require('./mlClient');
const { writeFeedEvent, writeMlStatus } = require('./firebaseActions');

const POLL_INTERVAL_MS = parseInt(process.env.ML_POLL_INTERVAL_MS || '250', 10);
const DEFAULT_SUCCESS_SECONDS = parseFloat(process.env.SIT_SUCCESS_SECONDS || '5');
const DEFAULT_TIMEOUT_SECONDS = parseFloat(process.env.SESSION_TIMEOUT_SECONDS || '180');

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
    grams: s.grams,
  };
}

function listSessions() {
  return Array.from(sessions.keys()).map((deviceId) => getPublicSession(deviceId));
}

function prettyPrintInference(deviceId, payload) {
  console.log('\n=== ML INFERENCE ===');
  console.log(
    JSON.stringify(
      {
        deviceId,
        ...payload,
      },
      null,
      2
    )
  );
}

function prettyPrintFinal(deviceId, finalPayload) {
  console.log('\n=== SESSION FINAL ===');
  console.log(
    JSON.stringify(
      {
        deviceId,
        ...finalPayload,
      },
      null,
      2
    )
  );
}

async function startSession(deviceId, options = {}) {
  if (!deviceId) {
    throw new Error('deviceId is required');
  }

  const existing = sessions.get(deviceId);
  if (existing?.active) {
    return getPublicSession(deviceId);
  }

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
    sitConfidenceSum: 0,
    sitConfidenceCount: 0,
    timeoutSeconds: Number.isFinite(options.timeoutSeconds)
      ? options.timeoutSeconds
      : DEFAULT_TIMEOUT_SECONDS,
    successSeconds: Number.isFinite(options.successSeconds)
      ? options.successSeconds
      : DEFAULT_SUCCESS_SECONDS,
  };

  sessions.set(deviceId, session);
  console.log(`[session] started ${deviceId}`);

  await writeMlStatus(deviceId, {
    status: 'running',
    event: { type: 'session_started' },
    session_started_at: session.startedAt,
    timeout_seconds: session.timeoutSeconds,
    success_seconds: session.successSeconds,
    updatedAt: Date.now(),
  });

  runLoop(deviceId).catch((err) => {
    console.error(`[session] loop crashed for ${deviceId}:`, err);
    finishSession(deviceId, {
      type: 'session_error',
      error: String(err?.message || err),
    }).catch((inner) => console.error('finishSession error:', inner));
  });

  return getPublicSession(deviceId);
}

async function stopSession(deviceId, event = { type: 'session_stopped' }) {
  await finishSession(deviceId, event);
  return getPublicSession(deviceId);
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

      console.log(`[session] calling ML for ${deviceId}, frameId=${frameId}`);
      const ml = await predictFrame(latestJpeg);

      if (!ml) {
        await sleep(POLL_INTERVAL_MS);
        continue;
      }

      session.lastInferenceAt = Date.now();

      const posture = ml.posture || 'unknown';
      const dogDetected = !!ml.dog_detected;

      const sitScore =
        typeof ml.sit_probability === 'number'
          ? ml.sit_probability
          : (typeof ml.confidence === 'number' ? ml.confidence : null);

      if (dogDetected && posture === 'sitting') {
        if (!session.sitStartedAt) {
          session.sitStartedAt = session.lastInferenceAt;
          session.sitConfidenceSum = 0;
          session.sitConfidenceCount = 0;
        }

        if (sitScore !== null) {
          session.sitConfidenceSum += sitScore;
          session.sitConfidenceCount += 1;
        }
      } else {
        session.sitStartedAt = null;
        session.sitConfidenceSum = 0;
        session.sitConfidenceCount = 0;
      }

      const sitDurationSec = session.sitStartedAt
        ? (session.lastInferenceAt - session.sitStartedAt) / 1000
        : 0;

      const avgSitConfidence =
        session.sitConfidenceCount > 0
          ? session.sitConfidenceSum / session.sitConfidenceCount
          : null;

      const payload = {
        ...ml,
        dog_detected: dogDetected,
        posture,
        sit_duration_sec: sitDurationSec,
        avg_sit_confidence: avgSitConfidence,
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

      prettyPrintInference(deviceId, payload);
      await writeMlStatus(deviceId, payload);

      if (payload.event?.type === 'sit_success') {
        await finishSession(deviceId, {
          type: 'sit_success',
          session_duration_sec: elapsedSec,
          sit_duration_sec: sitDurationSec,
          sit_probability: payload.sit_probability,
          confidence: payload.confidence,
          avg_sit_confidence: avgSitConfidence,
        });
        break;
      }
    } catch (err) {
      console.error(`[session] inference error for ${deviceId}:`, err);

      const errorPayload = {
        status: 'error',
        event: { type: 'inference_error', message: String(err?.message || err) },
        updatedAt: Date.now(),
      };

      prettyPrintInference(deviceId, errorPayload);
      await writeMlStatus(deviceId, errorPayload);
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

  prettyPrintFinal(deviceId, finalPayload);

  await writeMlStatus(deviceId, finalPayload);
  await writeFeedEvent(deviceId, {
    ...finalPayload,
  });

  if (event.type === 'sit_success') {
    console.log('[session] SUCCESS average sit confidence:', event.avg_sit_confidence);
  }

  console.log(`[session] finished ${deviceId}: ${event.type}`);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

module.exports = {
  startSession,
  stopSession,
  getSession,
  getPublicSession,
  listSessions,
};