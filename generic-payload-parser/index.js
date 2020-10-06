const {lightningPayloadParser} = require('@ucd-lib/goes-r-packet-decoder');
const fs = require('fs');
const path = require('path');
const project = require('./lib/project')
const APIDS = ['301', '302'];

let file = process.argv[2];
let fileParsed = path.parse(file);
let apid = fileParsed.dir.split('/').pop();

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

    let summary = {
      total_energy : 0,
      event_count : outcontent.length
    }
    outcontent.forEach(e => summary.total_energy += e.event_energy);

    fs.writeFileSync(path.join(fileParsed.dir, 'summary.json'), JSON.stringify(summary));
  }

  fs.writeFileSync(outfile, JSON.stringify(outcontent));
})();