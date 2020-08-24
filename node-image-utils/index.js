const fs = require('fs-extra');
const path = require('path');

if( process.argv[2] === 'jp2-to-png' ) {
  jp2ToPng();
} else {
  console.error('Unknown command: '+process.argv[2]);
  process.exit(-1);
}

async function jp2ToPng() {
  let rootDir = process.argv.length > 2 ? path.parse(process.argv[3]).dir : process.cwd();
  let metadata = require(rootDir+'/fragment-metadata.json');
  let data = {};
  for( let i = 0; i < metadata.fragmentsCount; i++ ) {
    console.log('Reading file: '+path.join(rootDir, 'fragments', i+'', 'image_fragment.jp2'));
    data['fragment_data_'+i] = {
      data : await fs.readFile(path.join(rootDir, 'fragments', i+'', 'image_fragment.jp2'))
    }
  }

  const fn = require("./lib/jp2-to-png");
  const compositePng = await fn(metadata, data);

  console.log('Writing file: '+path.join(rootDir, 'image.png'));
  await fs.writeFile(path.join(rootDir, 'image.png'), compositePng);
}