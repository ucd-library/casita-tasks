const JpgImage = require('./jpxInt16');
// const Jimp = require('jimp');
const PNG = require('pngjs').PNG;
const config = require('../config');

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
  
  let conversionFactor = config.worker16Conversion[metadata.apid];

  // let histo = {};
  let localmax = 0;

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
    let j;
    let crow = fmetadata.ROW_OFFSET_WITH_IMAGE_BLOCK*imageWidth*4;
    for( j = 0; j < tiles[0].length; j++ ) {

      // min seems like: 32792
      // max seems like: 33791, this is high, using a lower number

      // let val = (tiles[0][j]/20) - 1550; // - (32500/8);
      let val = Math.round(((tiles[0][j] - 32790) / conversionFactor) * 255);

      // if( !histo[tiles[0][j]]) histo[tiles[0][j]] = 1;
      // else histo[tiles[0][j]] += 1;
      
      // used to figure out above
      if( metadata.apid === 'b1' ) {
        if( max < tiles[0][j] ) max = tiles[0][j];
        if( localmax < tiles[0][j] ) localmax = tiles[0][j];
        if( min > tiles[0][j] ) min = tiles[0][j];
      }
      
      if( val > 255 ) val = 255;
      else if( val < 0 ) val = 0;

      // if( (tiles[0][j]/20) > 1800 ) {
      //   png.data[(crow)+(j*4)] = 0;
      //   png.data[(crow)+(j*4)+1] = 0;
      //   png.data[(crow)+(j*4)+2] = 0;
      //   png.data[(crow)+(j*4)+3] = 0;
      //   continue;
      // }

      png.data[(crow)+(j*4)] = val;
      png.data[(crow)+(j*4)+1] = val;
      png.data[(crow)+(j*4)+2] = val;
      png.data[(crow)+(j*4)+3] = 255;
    }

    // increment the current row we should be rendering
    rowOffset += jpgImage.height;
  }


  // console.log(
  //   Math.min(... Object.keys(histo)),
  //   Math.max(... Object.keys(histo))
  // )

  let fulldata = PNG.sync.write(png);
  // let image = await Jimp.read(fulldata);
  // image.resize(
  //   Math.ceil(imageWidth/config.imageScaleFactor), 
  //   Math.ceil(imageHeight/config.imageScaleFactor)
  // );
  // data = await image.getBufferAsync(Jimp.MIME_PNG);

  return fulldata;
}
