import {config, utils} from '@ucd-lib/casita-worker'; 
import decoder from '@ucd-lib/goes-r-packet-decoder';
import fs from 'fs';
import path from 'path';
import project from './lib/project.js';

const {lightningPayloadParser} = decoder;

async function run() {
  let metadata = utils.getDataFromPath(config.file);
  let data = fs.readFileSync(config.file);
  let outfile = path.join(path.parse(config.file).dir, 'payload.json');
  let outcontent = {};

  if( metadata.apid === '302' ) {
    outcontent = lightningPayloadParser.parseFlashData(data);
    for( let flash of outcontent ) {
      let xy = await project(flash.flash_lon, flash.flash_lat);
      flash.flash_x = xy.x;
      flash.flash_y = xy.y;
    }
  } else if( metadata.apid === '301' ) {
    outcontent = lightningPayloadParser.parseEventData(data);
  }

  fs.writeFileSync(outfile, JSON.stringify(outcontent));

  metadata.files = [outfile.replace(config.fs.nfsRoot)];

  return metadata;
}

export default run();