const {PNG} = require('@ucd-lib/pngjs');
const fs = require('fs-extra');
const path = require('path');

const redFile = path.join(__dirname, 'test', 'web-scaled-2.png');
const greenFile = path.join(__dirname, 'test', 'web-scaled-3.png');
const blueFile = path.join(__dirname, 'test', 'web-scaled-1.png');

(async function() {

  let red = await read(redFile);
  let blue = await read(blueFile);
  let green = await read(greenFile);

  let colorData = Buffer.alloc(red.width*red.height*4);
  let cIndex;
  let length = red.width*red.height;

  for( let i = 0; i < length; i++ ) {
    cIndex = i*4;
    
    colorData[cIndex] = red.data[cIndex]*0.8;
    colorData[cIndex+1] = green.data[cIndex]*0.8;
    colorData[cIndex+2] = blue.data[cIndex];
    colorData[cIndex+3] = 255;
  }

  let colorPng = new PNG({
    colorType : 6,
    height: red.height,
    width: red.width
  });
  colorPng.data = colorData;

  colorData = PNG.sync.write(colorPng, {
    colorType : 6,
    height: red.height,
    width: red.width
  });

  fs.writeFileSync(path.join(__dirname, 'test','color.png'), colorData);
})()

async function read(file) {
  let imageData = await fs.readFile(file);
  return new Promise((resolve, reject) => {
    new PNG({ colorType: 0, inputColorType: 0 })
      .parse(imageData, function (error, data) {
        if( error ) reject(error);
        else resolve(data);
      });
  });
}