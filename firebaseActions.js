let admin = null;
let db = null;

function initFirebase(firebaseAdmin, database) {
  admin = firebaseAdmin;
  db = database;
}

async function writeDispenseCommand(deviceId, grams = 25) {
  if (!db) return;

  await db.ref(`commands/${deviceId}/pending`).set({
    id: `ml-${deviceId}-${Date.now()}`,
    action: 'dispense',
    grams,
    by: 'websocket-backend',
    ts: Date.now(),
    source: 'websocket-service',
  });
}

async function writeFeedEvent(deviceId, event) {
  if (!db) return;

  const ref = db.ref(`feedEvents/${deviceId}`).push();
  await ref.set({
    ...event,
    ts: event.ts ?? Date.now(),
  });
}

async function writeMlStatus(deviceId, result) {
  if (!db) return;
  await db.ref(`devices/${deviceId}/ml`).set(result);
}

module.exports = {
  initFirebase,
  writeDispenseCommand,
  writeFeedEvent,
  writeMlStatus,
};
