const fs = require('fs-extra');
const path = require('path');
const CaReproject = require('./lib/ca-reproject');
const jp2ToPngFn = require("./lib/jp2-to-png");

if( process.argv[2] === 'jp2-to-png' ) {
  jp2ToPng();
} else if( process.argv[2] === 'reproject' ) {
  reproject();
} else {
  console.error('Unknown command: '+process.argv[2]);
  process.exit(-1);
}

async function reproject() {
  let files = process.argv.splice(2, process.argv.length-2);
  
  const caReproject = new CaReproject(files);
  await caReproject.loadFiles();
  await caReproject.run();
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

  const compositePng = await jp2ToPngFn(metadata, data);

  console.log('Writing file: '+path.join(rootDir, 'image.png'));
  await fs.writeFile(path.join(rootDir, 'image.png'), compositePng);
  await fs.remove(path.join(rootDir, 'fragments'));
}