const fs = require('fs-extra');
const path = require('path');
const {PNG} = require('@ucd-lib/pngjs');
const config = require('./config');

class GRBBlockComposite {

  async run(dir) {
    if( path.parse(dir).base === 'image.png' ) {
      dir = path.parse(dir).dir;
    }
    if( !dir.match(/\/blocks/) ) {
      dir = path.join(dir, 'blocks');
    }
    if( !fs.existsSync(dir) ) {
      throw new Error('Directory does not exist: '+dir);
    }

    let info = await this.getCompositeInfo(dir);
    if( !info ) return null;

    let sciPngData = new Uint16Array(info.width*info.height);
    let webPngData = Buffer.alloc(info.width*info.height);

    let histogram = {};
    for( let block of info.blocks ) {
      await this.addBlock(dir, block, info.width, sciPngData, webPngData, histogram);
      // break;
    }

    let keys = Object.keys(histogram).map(i => parseInt(i));

    keys.sort();
    keys = keys.splice(1, keys.length-2);

    let min = 99999999;
    let max = 0;
    for( let i = 0; i < keys.length; i++ ) {
      if( min > keys[i] ) min = keys[i];
      if( max < keys[i] ) max = keys[i];
    }

    let spread = max-min;
    for( let i = 0; i < sciPngData.length; i++ ) {
      webPngData[i] = (((sciPngData[i] - min) / spread) * 255);
      if( webPngData[i] > 255 ) webPngData[i] = 255;
      if( webPngData[i] < 0 ) webPngData[i] = 0;
    }

    let sciPng = new PNG({
      colorType: 0,
      inputColorType: 0,
      bitDepth : 16,
      height: info.height,
      width: info.width
    });
    let webPng = new PNG({
      colorType: 0,
      inputColorType: 0,
      height: info.height,
      width: info.width
    });

    sciPng.data = sciPngData;
    webPng.data = webPngData;

    return {
      sciPng : PNG.sync.write(sciPng, {
        colorType: 0,
        inputColorType: 0,
        bitDepth : 16,
        height: info.height,
        width: info.width
      }),
      webPng : PNG.sync.write(webPng, {
        colorType: 0,
        inputColorType: 0,
        height: info.height,
        width: info.width
      })
    };
  }

  async addBlock(dir, block, compositeWidth, sciPngData, webPngData, histogram) {
    if( !fs.existsSync(path.join(dir, block, 'fragment-metadata.json')) ) {
      console.warn('Failed to locate block metadata: '+path.join(dir, block, 'fragment-metadata.json'));
      return;
    }
    let blockMetadata = await this.readMetadata(
      path.join(dir, block, 'fragment-metadata.json')
    );
    let imageMetadata = blockMetadata.imagePayload;
    
    if( !fs.existsSync(path.join(dir, block, 'image.png')) ) {
      console.warn('Failed to locate block data: '+path.join(dir, block, 'image.png'));
      return;
    }
    let imageData = await this.readPng(
      path.join(dir, block, 'image.png')
    );

    let row = imageMetadata.UPPER_LOWER_LEFT_Y_COORDINATE;
    let col = imageMetadata.UPPER_LOWER_LEFT_X_COORDINATE;
    let rval, val;

    let productDef = config.apidProducts[blockMetadata.apid] || {};
    let maxValue = productDef.maxValue || 65536;
    let scale = productDef.scale || 1;
    let spread = maxValue;
    if( productDef.offsetBounds ) {
      spread = productDef.offsetBounds.max - productDef.offsetBounds.min;
    }

    let offset = 0, loc;

    // PNGJS always returns RGBA array, even if source was grayscale
    for( let j = 0; j < imageData.data.length; j += 4 ) {

      rval = imageData.data[j];
      // val = Math.round(((rval*scale) / spread) * 255);

      // if( val > 255 ) val = 255;
      // else if( val < 0 ) val = 0;

      if( !histogram[rval] ) histogram[rval] = 0;
      histogram[rval]++;

      if( offset !== 0 && offset % imageMetadata.IMAGE_BLOCK_WIDTH === 0 ) {
        row++;
      }

      loc = (row * (compositeWidth)) + // current row (y) offset 
       col + // column (x) offset of block
       (offset % imageMetadata.IMAGE_BLOCK_WIDTH ); // column (x) offset within block

      webPngData[loc] = 0;
      sciPngData[loc] = rval;

      offset++;
    }
  }

  async readMetadata(file) {
    return JSON.parse(await fs.readFile(file, 'utf-8'));
  }

  async readPng(file, opts) {
    // assume 16bit grayscale
    if( !opts ) {
      opts = {
        colorType: 0,
        inputColorType: 0,
        skipRescale : true,
        bitDepth : 16
      }
    }

    let imageData = await fs.readFile(file);

    // return sync.read(imageData, opts);
    return new Promise((resolve, reject) => {
      new PNG(opts).parse(imageData, function (error, data) {
        if( error ) reject(error);
        else resolve(data);
      });
    });
  }

  async getCompositeInfo(dir) {
    let max = {x: 0, y: 0};
    let blocks = await fs.readdir(dir);
    
    if( !blocks.length ) {
      console.warn('No blocks found in directory: '+dir);
      return
    }

    for( let block of blocks ) {
      let [x, y] = block.split('-').map(coord => parseInt(coord));
      
      if( x > max.x ) max.x = x;
      if( y > max.y ) max.y = y;
    }

    if( !fs.existsSync(path.join(dir, blocks[0], 'fragment-metadata.json')) ) {
      console.warn('Failed to locate init block metadata: '+path.join(dir, blocks[0], 'fragment-metadata.json'));
      return null;
    }

    let maxMetadata = await this.readMetadata(
      path.join(dir, blocks[0], 'fragment-metadata.json')
    );
    let imageMetadata = maxMetadata.imagePayload;
    
    return {
      blocks,
      width: max.x + imageMetadata.IMAGE_BLOCK_WIDTH,
      height: max.y + imageMetadata.IMAGE_BLOCK_HEIGHT
    }
  }

}

module.exports = new GRBBlockComposite();