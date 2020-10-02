const app = require('express')();
const {logger, config} = require('@ucd-lib/krm-node-utils');
const fs = require('fs');
const path = require('path');

const EXPIRED = 1000 * 60 * 3;
const STREAMS = ['decoded', 'secdecoded'];
const FILENAME = 'stream-status.json';
const STATUS_FILE = path.join(config.fs.nfsRoot, FILENAME);

app.all((req, res, next) => {
  console.log(req.originalPath);
  next();
})
app.get('/_/status/test', (req, res) => {
  console.log('here');
  res.send('test')
})
app.get('/_/status/channel/:channel/:expire?', handleRequest);

async function handleRequest(req, res) {
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
}

async function readFile() {
  if( !fs.existsSync(STATUS_FILE) ) return {};
  return JSON.parse(fs.readFileSync(STATUS_FILE, 'utf-8'));
}

async function isChannelAlive(channel, expired) {
  let status = await readFile();
  if( !status.stream ) return {channel, status: false};
  if( !status.stream[STREAMS[channel-1]] ) return {channel, status: false};
  let time = status.stream[STREAMS[channel-1]].serverTime;
  if( !time ) return {channel, status: false};
  time = new Date(time);

  if( time.getTime() < Date.now() - expired ) {
    return {
      channel,
      status: false, 
      serverTime : new Date(),
      thresholdTime: new Date(Date.now() - expired), 
      lastProductReceived: time
    }
  }

  return {
    channel,
    status: true, 
    serverTime : new Date(),
    thresholdTime: new Date(Date.now() - expired), 
    lastProductReceived: time
  }
}

app.listen(3000, () => {
  logger.info('listening on *:3000');
});