
// 16 bit jp2 (12/10 bit data) to 8 bit png conversion
const BIT_CONVERSION = {
  '12' : 3000, // 12bit (4096) but data range seems capped to 3000
  '10' : 750  // 10bit (1024) but data range seems capped to 750
}

module.exports = {
  worker16Conversion : {
    'b1' : BIT_CONVERSION['12'],
    'b2' : BIT_CONVERSION['10'],
    '91' : BIT_CONVERSION['12'],
    '92' : BIT_CONVERSION['10']
  },

  caBlocks : {
    92 : ['1314-6328']
  }
}