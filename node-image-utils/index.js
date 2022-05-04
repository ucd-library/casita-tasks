const fs = require('fs-extra');
const path = require('path');
const CaReproject = require('./lib/ca-reproject');
const jp2ToPngFn = require("./lib/jp2-to-png");
const scaleFn = require('./lib/scale');
const Composite = require('./lib/composite');
const {logger} = require('@ucd-lib/krm-node-utils');

(async function() {
  try {
    if( process.argv[2] === 'jp2-to-png' ) {
      await jp2ToPng();
    } else if( process.argv[2] === 'composite' ) {
      await composite();
    } else if( process.argv[2] === 'reproject' ) {
      await reproject();
    } else if( process.argv[2] === 'scale' ) {
      await scale();
    } else {
      logger.error('Unknown command: '+process.argv[2]);
      setTimeout(() => process.exit(-1), 50);
    }
  } catch(e) {
    logger.error('Failed to run command: ', process.argv, e);
    setTimeout(() => process.exit(-1), 50);
  }
})();

async function scale() {
  let file = process.argv[3];
  let band = process.argv[4];

  try {
    let image = await scaleFn(file, band);
    let newFile = path.join(path.parse(file).dir, 'web-scaled.png');
    console.log('Writing scale file: '+newFile);
    await fs.writeFile(newFile, image);
  } catch(e) {
    console.error('Failed to scale image: ', file, band, e);
  }

}

async function composite() {
  let rootDir = process.argv.length > 2 ? process.argv[3] : process.cwd();
  let images = await Composite.run(rootDir);
  if( !images ) {
    console.warn('Failed to create composite image');
    return;
  }

  if( path.parse(rootDir).base === 'image.png' ) {
    rootDir = path.parse(rootDir).dir;
  }
  if( rootDir.match(/\/blocks/) ) {
    rootDir = path.resolve(rootDir, '..');
  }

  console.log('Writing composite file: '+path.join(rootDir, 'image.png'));
  await fs.writeFile(path.join(rootDir, 'image.png'), images.sciPng);

  console.log('Writing composite file: '+path.join(rootDir, 'web.png'));
  await fs.writeFile(path.join(rootDir, 'web.png'), images.webPng);
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
    console.log('Reading file: '+path.join(rootDir, 'fragments', i+'', 'image-fragment.jp2'));
    data['fragment_data_'+i] = {
      data : await fs.readFile(path.join(rootDir, 'fragments', i+'', 'image-fragment.jp2'))
    }
    metadata[`fragment_headers_${i}`] = JSON.parse(
      await fs.readFile(path.join(rootDir, 'fragments', i+'', 'image-fragment-metadata.json'), 'utf-8')
    );
  }

  const images = await jp2ToPngFn(metadata, data);

  console.log('Writing file: '+path.join(rootDir, 'image.png'));
  await fs.writeFile(path.join(rootDir, 'image.png'), images.sciPng);

  console.log('Writing file: '+path.join(rootDir, 'web.png'));
  await fs.writeFile(path.join(rootDir, 'web.png'), images.webPng);

  if( process.argv.includes('--rm-fragments') ) {
    await fs.remove(path.join(rootDir, 'fragments'));
  }
}