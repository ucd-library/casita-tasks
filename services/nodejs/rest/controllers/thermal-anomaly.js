import express from 'express';
import model from '../../../../tasks/nodejs/thermal-anomaly/query.js';
const router = express.Router();

router.get('/events', async (req, res) => {
  try {
    res.json(await model.getEvents());
  } catch(e) {
    res.status(500).json({
      error: true,
      message: e.message,
      stack: e.stack
    })
  }
});

router.get('/event/:id', async (req, res) => {
  try {
    res.json(await model.getEvent(req.params.id));
  } catch(e) {
    res.status(500).json({
      error: true,
      message: e.message,
      stack: e.stack
    })
  }
});

export default router;