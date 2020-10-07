const {lightningPayloadParser} = require('@ucd-lib/goes-r-packet-decoder');
const fs = require('fs');
const path = require('path');
const project = require('./lib/project')
const APIDS = ['301', '302'];

let file = process.argv[2];
let fileParsed = path.parse(file);
let apid = fileParsed.dir.split('/').pop();

// handle 301 (lightning event) summary
if( apid === '301' && fileParsed.base === 'stats.json' ) {

  let summary = {
    total_energy : 0,
    event_count : 0
  }

  let rootDir = path.resolve(fileParsed.dir, '..', '..');
  let dirs = fs.readdirSync(rootDir);

  for( let msdir of dirs ) {
    if( msdir === 'summary' ) continue;
    let data = JSON.parse(
      fs.readFileSync(path.join(rootDir, msdir, '301', 'payload.json'), 'utf-8')
    );
    summary.event_count += data.length;
    data.forEach(e => summary.total_energy += e.event_energy);
  }

  fs.writeFileSync(file, JSON.stringify(summary));
  return;
}


if( fileParsed.base !== 'payload.bin' ) return;
if( !APIDS.includes(apid) ) process.exit();

let data = fs.readFileSync(file);
let outfile = path.join(fileParsed.dir, 'payload.json');
let outcontent = {};

(async function() {
  if( apid === '302' ) {
    outcontent = lightningPayloadParser.parseFlashData(data);
    for( let flash of outcontent ) {
      let xy = await project(flash.flash_lon, flash.flash_lat);
      flash.flash_x = xy.x;
      flash.flash_y = xy.y;
    }
  } else if( apid === '301' ) {
    outcontent = lightningPayloadParser.parseEventData(data);
  }

  fs.writeFileSync(outfile, JSON.stringify(outcontent));
})();