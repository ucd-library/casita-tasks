const fs = require('fs-extra');
const path = require('path');
const {PNG} = require('@ucd-lib/pngjs');
const config = require('./config');

module.exports = function scale(file, band=1) {
  let info = config.bandResolutions[parseInt(band)];
  let scale = (info.resolution * 25) / 100;

  let imageData = await readPng(file);

  scale = (scale/100);
  let newSizeW = Math.floor(imageData.width * scale);
  let newSizeH = Math.floor(imageData.height * scale);
  let webPngData = Buffer.alloc(newSizeW*newSizeH);

  let row, col;
  for( row = 0; row < newSizeH; row++ ) {
    for( col = 0; col < newSizeW; col++ ) {
      webPngData[(newSizeW*row)+col] = getPxData(imageData, row/scale, col/scale);
    }
  }

  let webPng = new PNG({
    colorType: 0,
    inputColorType: 0,
    height: newSizeH,
    width: newSizeW
  });

  webPng.data = webPngData;

  return PNG.sync.write(webPng, {
    colorType: 0,
    inputColorType: 0,
    height: newSizeH,
    width: newSizeW
  });
}

function getPxData(imageData, row, col) {
  let sum = 0;
  let count = 0;
  let i, j, loc;

  for( i = -1; i <= 1; i++ ) {
    for( j = -1; j <= 1; j++ ) {
      if( row + i < 0 ) continue;
      if( row + i >= imageData.width ) continue;
      if( col + j < 0 ) continue;
      if( col + j >= imageData.height ) continue;

      loc = (imageData.width * (row+i)) + (col + j);
      sum += imageData.data[loc*4];
      count += 1;
    }
  }

  if( count === 0 ) return 0;
  return Math.round(sum/count);
}

async function readPng(file, opts) {
  // assume 8bit grayscale
  if( !opts ) {
    opts = {
      colorType: 0,
      inputColorType: 0,
      skipRescale : true,
      bitDepth : 8
    }
  }

  let imageData = await fs.readFile(file);

  return new Promise((resolve, reject) => {
    new PNG(opts).parse(imageData, function (error, data) {
      if( error ) reject(error);
      else resolve(data);
    });
  });
}