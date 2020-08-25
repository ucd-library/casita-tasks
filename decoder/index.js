const Processor = require('@ucd-lib/goes-r-packet-decoder/lib/binary-stream-processor');

let target = process.env.TARGET_URL || 'http://localhost:3000'

let processor = new Processor({
  consoleLogStatus : true,
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

processor.pipe();