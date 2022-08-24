import express from 'express';
import model from '../../notifications/lib/web-push.js';
const router = express.Router();

router.post('/register', async (req, res) => {
  try {
    let success = await model.register(
      req.body.type,
      req.body.subscription
    );
    res.json({success});
  } catch(e) {
    res.status(500).json({
      error: true,
      message: e.message,
      stack: e.stack
    })
  }
});

router.post('/registrations/:endpointUrl', async (req, res) => {
  try {
    res.json(await model.getNotifications(
      decodeURIComponent(req.params.endpointUrl)
    ));
  } catch(e) {
    res.status(500).json({
      error: true,
      message: e.message,
      stack: e.stack
    })
  }
});

router.delete('/unregister/:endpointUrl/:type?', async (req, res) => {
  try {
    res.json(await model.unregister(
      req.params.endpointUrl, req.params.type
    ));
  } catch(e) {
    res.status(500).json({
      error: true,
      message: e.message,
      stack: e.stack
    })
  }
});

export default router;