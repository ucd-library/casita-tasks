const config = require('../lib/config');
const logger = require('../lib/logger');
const path = require('path');
const jp2ToPng = require("./lib/jp2-to-png");

async function run() {
  let rootDir = config.directory;

  let metadata = require(rootDir+'/fragment-metadata.json');
  let data = {};
  for( let i = 0; i < metadata.fragmentsCount; i++ ) {
    logger.debug('Reading file: '+path.join(rootDir, 'fragments', i+'', 'image-fragment.jp2'));
    data['fragment_data_'+i] = {
      data : await fs.readFile(path.join(rootDir, 'fragments', i+'', 'image-fragment.jp2'))
    }
    metadata[`fragment_headers_${i}`] = JSON.parse(
      await fs.readFile(path.join(rootDir, 'fragments', i+'', 'image-fragment-metadata.json'), 'utf-8')
    );
  }

  const images = await jp2ToPng(metadata, data);

  logger.debug('Writing file: '+path.join(rootDir, 'image.png'));
  await fs.writeFile(path.join(rootDir, 'image.png'), images.sciPng);

  logger.debug('Writing file: '+path.join(rootDir, 'web.png'));
  await fs.writeFile(path.join(rootDir, 'web.png'), images.webPng);

  return {
    files : [path.join(rootDir, 'image.png'), path.join(rootDir, 'web.png') ]
  }
}

module.exports = run;