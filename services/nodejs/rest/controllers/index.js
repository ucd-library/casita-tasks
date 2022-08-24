import express from 'express';
import status from './status.js';
import webPush from './web-push.js';
import thermalAnomaly from './thermal-anomaly.js';
const router = express.Router();

router.use('/status', status);
router.use('/web-push', webPush);
router.use('/thermal-anomaly', thermalAnomaly);

export default router;