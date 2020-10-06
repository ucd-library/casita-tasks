const {lightningPayloadParser} = require('@ucd-lib/goes-r-packet-decoder');
const fs = require('fs');
const path = require('path');
const APIDS = ['301', '302'];

let file = process.argv[2];
let fileParsed = path.parse(file);
let apid = fileParsed.dir.split('/').pop();

if( fileParsed.base !== 'payload.bin' ) return;
if( !APIDS.includes(apid) ) process.exit();

let data = fs.readFileSync(file);
let outfile = path.join(fileParsed.dir, 'payload.json');
let outcontent = {};

if( apid === '302' ) {
  outcontent = lightningPayloadParser.parseFlashData(data);
} else if( apid === '301' ) {
  outcontent = lightningPayloadParser.parseEventData(data);
}

fs.writeFileSync(outfile, JSON.stringify(outcontent));