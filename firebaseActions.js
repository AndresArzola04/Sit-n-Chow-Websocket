const adminSdk = require('firebase-admin');

let admin = null;
let db = null;

function initFirebase() {
  if (admin && db) {
    return { admin, db };
  }

  const dbUrl = process.env.FIREBASE_DB_URL;
  const serviceAccountJson = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;

  if (!dbUrl || !serviceAccountJson) {
    console.warn('Firebase env vars missing; Firebase disabled');
    return { admin: null, db: null };
  }

  try {
    const serviceAccount = JSON.parse(serviceAccountJson);

    if (!adminSdk.apps.length) {
      adminSdk.initializeApp({
        credential: adminSdk.credential.cert(serviceAccount),
        databaseURL: dbUrl,
      });
    }

    admin = adminSdk;
    db = adminSdk.database();
    console.log('Firebase initialized');
    return { admin, db };
  } catch (err) {
    console.error('Failed to initialize Firebase:', err);
    return { admin: null, db: null };
  }
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