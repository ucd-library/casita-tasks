import express from 'express';
import {pg, config} from '@ucd-lib/casita-worker';
const router = express.Router();

const EXPIRED = 1000 * 60 * 3;

router.get('/channel/:channel/:expire?', async (req, res) => {
  try {
    let channel = req.params.channel;
    let expire = req.params.expire || EXPIRED;
    let alive = await isChannelAlive(parseInt(channel), parseInt(expire));

    if( alive.status ) {
      res.status(200).json(
        Object.assign(alive, {status: 'up'})
      );
    } else {
      res.status(503).json(
        Object.assign(alive, {status: 'down'})
      );
    }

  } catch(e) {
    res.status(500).json({
      error : true,
      message : e.message,
      stack : e.stack
    });
  }
});

async function isChannelAlive(channel, expired) {
  channel = config.streams[parseInt(channel)-1];

  let result = await pg.query('select get_status_latest_timestamp($1) as last_received', [channel])

  if( !result.rows.length ) return {channel, status: false};
  let time = result.rows[0].last_received;

  let thresholdTime = Date.now() - expired;
  if( time.getTime() < thresholdTime ) {
    return {
      channel, expired,
      status: false, 
      serverTime : new Date(),
      thresholdTime: new Date(thresholdTime), 
      lastProductReceived: time
    }
  }

  return {
    channel, expired,
    status: true, 
    serverTime : new Date(),
    thresholdTime: new Date(thresholdTime), 
    lastProductReceived: time
  }
}

pg.connect();

export default router;