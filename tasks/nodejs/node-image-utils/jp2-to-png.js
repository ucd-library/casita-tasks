import {config, logger, utils } from '@ucd-lib/casita-worker';
import path from 'path';
import fs from 'fs-extra';
import jp2ToPng from "./lib/jp2-to-png.js";
import scale from './lib/scale.js';

async function run() {
  let rootDir = path.parse(config.metadataFile).dir;
  let metadata = JSON.parse(fs.readFileSync(config.metadataFile, 'utf-8'));
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

  let {satellite, product, date, hour, minsec, band} = rootDir.replace(config.fs.nfsRoot) 
  let scaleImage = await scale(path.join(rootDir, 'web.png'), band);
  let scaleFile = path.join(rootDir, 'web-scaled.png');
  logger.debug('Writing scale file: '+scaleFile);
  await fs.writeFile(scaleFile, scaleImage);

  let pathData = utils.getDataFromPath(rootDir);
  return {
    files : [
      path.join(rootDir, 'image.png'), 
      path.join(rootDir, 'web.png'),
      path.join(rootDir, 'web-scaled.png')
    ],
    ...pathData
  }
}

export default run;