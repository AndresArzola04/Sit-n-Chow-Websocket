const ML_SERVICE_URL = (process.env.ML_SERVICE_URL || '').replace(/\/$/, '');

async function inferFrame(jpegBuffer) {
  if (!ML_SERVICE_URL) {
    throw new Error('ML_SERVICE_URL is not set');
  }

  const form = new FormData();
  const blob = new Blob([jpegBuffer], { type: 'image/jpeg' });
  form.append('file', blob, 'frame.jpg');

  const resp = await fetch(`${ML_SERVICE_URL}/predict`, {
    method: 'POST',
    body: form,
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`ML request failed: ${resp.status} ${text}`);
  }

  return await resp.json();
}

module.exports = { inferFrame };