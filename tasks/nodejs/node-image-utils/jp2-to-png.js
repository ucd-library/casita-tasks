import {config, logger, utils, fsCache } from '@ucd-lib/casita-worker';
import path from 'path';
import fs from 'fs-extra';
import jp2ToPng from "./lib/jp2-to-png.js";
import scale from './lib/scale.js';

async function run() {
  let rootDir = path.parse(config.metadataFile).dir;
  let metadata = JSON.parse(await fsCache.get(config.metadataFile), true);
  let data = {};

  for( let i = 0; i < metadata.fragmentsCount; i++ ) {
    logger.debug('Reading file: '+path.join(rootDir, 'fragments', i+'', 'image-fragment.jp2'));
    data['fragment_data_'+i] = {
      data : await fsCache.get(path.join(rootDir, 'fragments', i+'', 'image-fragment.jp2'))
    }
    console.log(data['fragment_data_'+i]);
    metadata[`fragment_headers_${i}`] = JSON.parse(
      await fsCache.get(path.join(rootDir, 'fragments', i+'', 'image-fragment-metadata.json'), true)
    );
  }

  const images = await jp2ToPng(metadata, data);

  await fs.mkdirp(rootDir);
  await fs.writeFile(config.metadataFile, JSON.stringify(metadata));

  logger.debug('Writing file: '+path.join(rootDir, 'image.png'));
  await fs.writeFile(path.join(rootDir, 'image.png'), images.sciPng);

  logger.debug('Writing file: '+path.join(rootDir, 'web.png'));
  await fs.writeFile(path.join(rootDir, 'web.png'), images.webPng);

  let {band} = utils.getDataFromPath(rootDir);
  let scaleImage = await scale(path.join(rootDir, 'web.png'), parseInt(band));
  let scaleFile = path.join(rootDir, 'web-scaled.png');
  logger.debug('Writing scale file: '+scaleFile);
  await fs.writeFile(scaleFile, scaleImage);

  let pathData = utils.getDataFromPath(rootDir);
  rootDir = rootDir.replace(config.fs.nfsRoot, '');
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