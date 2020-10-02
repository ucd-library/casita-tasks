const fs = require('fs-extra');
const path = require('path');
const STREAMS = ['decoded', 'secdecoded'];
const FILENAME = 'stream-status.json';

let rootDir = process.argv[2];
let metadataFile = process.argv[3];

if( !metadataFile.startsWith(rootDir) ) {
  metadataFile = path.join(rootDir, metadataFile);
}

function getCurrentStatus() {
  let file = path.join(rootDir, FILENAME);
  if( fs.existsSync(file) ) {
    return readJsonFile(file);
  }
  
  let data = {
    stream : {},
    apid : {}
  }
  STREAMS.forEach(s => data.stream[s] = {});
  return data;
}

async function readJsonFile(file) {
  return JSON.parse(await fs.readFile(file, 'utf-8'));
}

(async function() {
  let data = await readJsonFile(metadataFile, 'utf-8');
  let status = await getCurrentStatus();

  let serverTime = new Date();
  let captureTime = null;
  if( data.type === 'generic' ) {
    if( data.headers && data.headers.SECONDS_SINCE_EPOCH ) {
      captureTime = new Date(946728000000 + data.headers.SECONDS_SINCE_EPOCH*1000);
    }
  } else if( data.type === 'image' ) {
    if( data && 
        data.imagePayload && 
        data.imagePayload.SECONDS_SINCE_EPOCH ) {
      captureTime = new Date(946728000000 + data.fragment_headers_0.imagePayload.SECONDS_SINCE_EPOCH*1000);
    }
  }

  if( data.apid ) {
    if( !status.apid ) status.apid = {};
    status.apid[data.apid] = {
      serverTime, captureTime
    }
  }

  if( data.streamName ) {
    if( !status.stream ) status.stream = {};
    status.stream[data.streamName] = {
      serverTime, captureTime
    }
  }

  await fs.writeFile(
    path.join(rootDir, FILENAME),
    JSON.stringify(status, '  ', '  ')
  );

})();