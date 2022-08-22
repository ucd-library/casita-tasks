import express from 'express';
import status from './status.js';
import webPush from './web-push.js';
const router = express.Router();

router.use('/status', status);
router.use('/web-push', webPush);

export default router;