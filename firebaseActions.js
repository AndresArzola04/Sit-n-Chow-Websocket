let admin = null;
let db = null;

function initFirebase(firebaseAdmin, database) {
  admin = firebaseAdmin;
  db = database;
}

async function writeFeedEvent(deviceId, event) {
  if (!db) return;
  await db.ref(`feedEvents/${deviceId}`).set(event);
}

async function writeMlStatus(deviceId, result) {
  if (!db) return;
  await db.ref(`devices/${deviceId}/ml`).set(result);
}

module.exports = {
  initFirebase,
  writeFeedEvent,
  writeMlStatus,
};
