const path = require('path');
const fs = require('fs');
const Processor = require('@ucd-lib/goes-r-packet-decoder/lib/binary-stream-processor');

let target = process.env.TARGET_URL || 'http://localhost:3000'

let processor = new Processor({
  live: false,
  consoleLogStatus : true,
  // filter : /^91$/i,
  imageBlock : {
    post : {
      url : target
    }
  },
  generic : {
    post : {
      url : target
    }
  }
})
processor.pipe(fs.createReadStream(path.join(__dirname, 'testsecdecorded.dat')));

setTimeout(() => process.exit(), 4000);