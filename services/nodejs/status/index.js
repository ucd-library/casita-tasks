import express from 'express';
import fs from 'fs';
import path from 'path';
import {logger} from '@ucd-lib/casita-worker';

const app = express();

const EXPIRED = 1000 * 60 * 3;
const STREAMS = ['decoded', 'secdecoded'];
// const FILENAME = 'stream-status.json';
// const STATUS_FILE = path.join(config.fs.nfsRoot, FILENAME);

const COLLECTION = 'stream-status';

app.use('/_/status/dashboard', express.static(path.join(__dirname,'static')));
app.get('/_/status/test', (req, res) => {
  res.send('ok');
})

app.get('/_/status/channel/:channel/:expire?', async (req, res) => {
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

app.get('/_/status/timing/apid/:apid/:start?/:stop?', async (req, res) => {
  try {
    let query = {$and : [{
      apid : req.params.apid,
    }]};

    if( req.params.start ) {
      query.$and.push({serverTime : { 
        $gte : new Date(req.params.start) 
      }}) 
    }
    if( req.params.end ) {
      query.$and.push({serverTime : { 
        $lte : new Date(req.params.end) 
      }}) 
    }

    await streamQuery(
      res, query,
      {projection: {_id:0, metadataFile:0}},
      {serverTime: 1}
    )

  } catch(e) {
    res.status(500).json({
      error : true,
      message : e.message,
      stack : e.stack
    });
  }
});

// app.get('/_/status/timing/all', async (req, res) => {
//   try {
//     await streamQuery(
//       res, {_id : {$ne : 'overview'}},
//       {projection: {_id:0, metadataFile:0}},
//       {serverTime: 1}
//     )

//   } catch(e) {
//     res.status(500).json({
//       error : true,
//       message : e.message,
//       stack : e.stack
//     });
//   }
// });

async function streamQuery(res, query, options={}, sort) {
  res.set('content-type', 'application/json');
  res.status(200);
  res.write('[');

  let collection = await mongo.getCollection(COLLECTION);
  let cursor = await collection.find(query, options) 
  if( sort ) cursor.sort();

  let document;
  let c = 0;
  while ( (document = await cursor.next()) ) {
    if( c > 0 ) res.write(',');
    res.write(JSON.stringify(document));
    c++;
  }
  res.write(']');
  res.end();
}

async function isChannelAlive(channel, expired) {
  let collection = await mongo.getCollection(COLLECTION);
  let status = await collection.findOne({_id: 'overview'});

  if( !status ) return {channel, status: false};
  if( !status.stream ) return {channel, status: false};
  if( !status.stream[STREAMS[channel-1]] ) return {channel, status: false};
  let time = status.stream[STREAMS[channel-1]].serverTime;
  if( !time ) return {channel, status: false};

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