import express from 'express';
import status from './status.js';
const router = express.Router();

router.use('/status', status);

export default router;