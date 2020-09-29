const app = require('express')();
const {logger, config} = require('@ucd-lib/krm-node-utils');
const fs = require('fs');
const path = require('path');

const EXPIRED = 1000 * 60 * 3;
const STREAMS = ['decoded', 'secdecoded'];
const FILENAME = 'stream-status.json';
const STATUS_FILE = path.join(config.fs.nfsRoot, FILENAME);

app.get('/channel/1', async (req, res) => {

  
});

app.get('/channel/2', (req, res) => {

});

async function handleRequest(req, res) {
  try {
    let channel = req.originPath.split('/').pop();
    let alive = await isChannelAlive(parseInt(channel));

    if( alive ) {
      res.status(200).json({
        status : 'up',
        channel
      });
    } else {
      res.status(503).json({
        status : 'down',
        channel
      });
    }

  } catch(e) {
    res.status(500).json({
      error : true,
      message : e.message,
      stack : e.stack
    });
  }
}

async function readFile() {
  if( !fs.existsSync(STATUS_FILE) ) return {};
  return JSON.parse(await fs.readFile(STATUS_FILE, 'utf-8'));
}

async function isChannelAlive(channel) {
  let status = await readFile();
  if( !status.stream ) return false;
  if( !status.stream[STREAMS[channel]] ) return false;
  let time = status.stream[STREAMS[channel]].serverTime;
  if( !time ) return false;
  time = new Date(time);

  if( time.getTime() < Date.now() - EXPIRED ) {
    return false;
  }

  return true;
}

app.listen(3000, () => {
  logger.info('listening on *:3000');
});