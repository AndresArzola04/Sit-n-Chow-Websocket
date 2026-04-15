const axios = require('axios');
const FormData = require('form-data');

const BASE_URL = process.env.ML_SERVICE_URL || 'http://localhost:8000';
const PREDICT_PATH = process.env.ML_PREDICT_PATH || '/predict';

async function predictFrame(jpegBuffer) {
  try {
    const form = new FormData();
    form.append('file', jpegBuffer, {
      filename: 'frame.jpg',
      contentType: 'image/jpeg'
    });

    const response = await axios.post(
      `${BASE_URL}${PREDICT_PATH}`,
      form,
      {
        headers: form.getHeaders(),
        timeout: 5000
      }
    );

    return response.data;
  } catch (err) {
    console.error('[ML ERROR]', err.response?.data || err.message);
    return null;
  }
}

module.exports = { predictFrame };