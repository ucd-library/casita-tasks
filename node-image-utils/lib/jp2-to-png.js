const JpgImage = require('./jpxInt16');
// const Jimp = require('jimp');
const PNG = require('pngjs').PNG;
const config = require('./config');

let max = 0;
let min = Number.MAX_SAFE_INTEGER;

module.exports = async (metadata, data) => {
  let fragmentCount = parseInt(metadata.fragmentsCount);
  let imageMetadata = JSON.parse(metadata.fragment_headers_0).imagePayload;
  let imageHeight = imageMetadata.IMAGE_BLOCK_HEIGHT;
  let imageWidth = imageMetadata.IMAGE_BLOCK_WIDTH;

  // create a blank png of same size
  let png = new PNG({
    colorType: 0,
    inputColorType: 0,
    // inputHasAlpha : false,
    filterType: 4,
    // bitDepth: 16,
    height: imageHeight,
    width: imageWidth
  });

  let rowOffset = 0;
  
  if( !config.apidProducts[metadata.apid] ) {
    console.warn(`No product definition for apid (${metadata.apid}), using full 16bit to 8bit conversion`);
  } else {
    console.log(`Using apid (${metadata.apid}) definition: ${JSON.stringify(config.apidProducts[metadata.apid])}`);
  }
  let productDef = config.apidProducts[metadata.apid] || {};
  let bitMask = productDef.bitMask || 0xFFFF;
  let maxValue = productDef.maxValue || 65536;

  for( let i = 0; i < fragmentCount; i++ ) {
    let fmetadata = JSON.parse(metadata[`fragment_headers_${i}`]).imagePayload;

    // parse the jp2 file
    let jpgImage = new JpgImage();
    try {
      jpgImage.parseCodestream(data[`fragment_data_${i}`].data, 0, data[`fragment_data_${i}`].data.length);
    } catch(e) {
      console.error(e);
      continue;
    }

    let tiles;
    if( Array.isArray(jpgImage.tiles[0]) ) {
      tiles = jpgImage.tiles[0].map(t => t.items);
    } else {
      tiles = jpgImage.tiles.map(t => t.items);
    }
    tiles[0] = new Uint16Array(tiles[0]);

    // fill missing fragment(s)
    for( let j = rowOffset; j < fmetadata.ROW_OFFSET_WITH_IMAGE_BLOCK-1; j++ ) {
      for( let z = 0; z < imageWidth; z++ ) {
        png.data[(j*imageWidth*4)+(z*4)] = 0;
        png.data[(j*imageWidth*4)+(z*4)+1] = 0;
        png.data[(j*imageWidth*4)+(z*4)+2] = 0;
        png.data[(j*imageWidth*4)+(z*4)+3] = 0;
      }
    }
    rowOffset = fmetadata.ROW_OFFSET_WITH_IMAGE_BLOCK-1;

    // fill fragment
    let j, val;
    let crow = fmetadata.ROW_OFFSET_WITH_IMAGE_BLOCK*imageWidth*4;
    for( j = 0; j < tiles[0].length; j++ ) {

      val = tiles[0][j] & bitMask;
      // val = maxValue - val;
      // debug
      // console.log(dec2bin(tiles[0][j]), tiles[0][j], dec2bin(val), val);
      val = Math.round((val / maxValue) * 255);

      // debugger;
      if( val > 255 ) val = 255;
      else if( val < 0 ) val = 0;

      png.data[(crow)+(j*4)] = val;
      png.data[(crow)+(j*4)+1] = val;
      png.data[(crow)+(j*4)+2] = val;
      png.data[(crow)+(j*4)+3] = 255;
    }

    // increment the current row we should be rendering
    rowOffset += jpgImage.height;
  }

  let fulldata = PNG.sync.write(png);

  return fulldata;
}