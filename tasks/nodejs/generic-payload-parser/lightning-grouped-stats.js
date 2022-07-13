import {config, utils} from '@ucd-lib/casita-worker';
import path from 'path';
import fs from 'fs';

async function run() {
  let file = path.join(config.folder, 'summary', 'stats.json');
  let fileParsed = path.parse(file);


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
  
  
  return
}

export default run;