
// 16 bit jp2 (12/10 bit data) to 8 bit png conversion
const BIT_CONVERSION = {
  '12' : 3000, // 12bit (4096) but data range seems capped to 3000
  '10' : 750  // 10bit (1024) but data range seems capped to 750
}

// https://www.goes-r.gov/users/docs/PUG-L1b-vol3.pdf
// 5.1.3.6.3 Radiances Product Quantity Characteristics
// Table 5.1.3.6.3-1 Radiances Product Quantity Characteristics
const BAND_CHARACTERISTICS = {
  1 : {
    resolution: 1,
    bitDepth: 10,
    bitMask : 0x3FF,
    maxValue : 1022
  },
  2 : { 
    resolution: 0.5,
    bitDepth: 12,
    bitMask : 0xFFF,
    maxValue : 4095
  },
  3 : { 
    resolution: 1,
    bitDepth: 10,
    bitMask : 0x3FF,
    maxValue : 1022
  },
  4 : { 
    resolution: 2,
    bitDepth: 11,
    bitMask: 0x7FF,
    maxValue : 2047
  },
  5 : { 
    resolution: 1,
    bitDepth: 10,
    bitMask : 0x3FF,
    maxValue : 1022
  },
  6 : { 
    resolution: 2,
    bitDepth: 10,
    bitMask : 0x3FF,
    maxValue : 1022
  },
  7 : { 
    resolution: 2,
    bitDepth: 14,
    bitMask : 0x1FFF,
    maxValue : 16383
  },
  8 : { 
    resolution: 2,
    bitDepth: 12,
    bitMask : 0xFFF,
    maxValue : 4095
  },
  9 : { 
    resolution: 2,
    bitDepth: 11,
    bitMask: 0x7FF,
    maxValue : 2047
  },
  10 : { 
    resolution: 2,
    bitDepth: 12,
    bitMask : 0xFFF,
    maxValue : 4095
  },
  11 : { 
    resolution: 2,
    bitDepth: 12,
    bitMask : 0xFFF,
    maxValue : 4095
  },
  12 : { 
    resolution: 2,
    bitDepth: 11,
    bitMask: 0x7FF,
    maxValue : 2047
  },
  13 : { 
    resolution: 2,
    bitDepth: 12,
    bitMask : 0xFFF,
    maxValue : 4095
  },
  14 : { 
    resolution: 2,
    bitDepth: 12,
    bitMask : 0xFFF,
    maxValue : 4095
  },
  15 : { 
    resolution: 2,
    bitDepth: 12,
    bitMask : 0xFFF,
    maxValue : 4095
  },
  16 : { 
    resolution: 2,
    bitDepth: 10,
    bitMask : 0x3FF,
    maxValue : 1022
  }
}

const BASE_CA_BLOCKS = [
  {top: 1314, left: 6328},
  {top: 1314, left: 7232},
  {top: 1820, left: 6328},
  {top: 1820, left: 7232}
];

for( let band in BAND_CHARACTERISTICS ) {
  let blocks = [];
  for( let block of BASE_CA_BLOCKS ) {
    blocks.push({
      top : block.top / BAND_CHARACTERISTICS[band].resolution,
      left : block.left / BAND_CHARACTERISTICS[band].resolution
    });
  }
  BAND_CHARACTERISTICS[band].blocks = blocks;
  BAND_CHARACTERISTICS[band].band = band;
}


const config = {
  bandResolutions : BAND_CHARACTERISTICS,
  baseCaBlocks : BASE_CA_BLOCKS,

  apidProducts : {
    // conus
    'b0' : BAND_CHARACTERISTICS['1'],
    'b1' : BAND_CHARACTERISTICS['2'],
    'b2' : BAND_CHARACTERISTICS['3'],
    'b3' : BAND_CHARACTERISTICS['4'],
    'b4' : BAND_CHARACTERISTICS['5'],
    'b5' : BAND_CHARACTERISTICS['6'],
    'b6' : BAND_CHARACTERISTICS['7'],
    'b7' : BAND_CHARACTERISTICS['8'],
    'b8' : BAND_CHARACTERISTICS['9'],
    'b9' : BAND_CHARACTERISTICS['10'],
    'ba' : BAND_CHARACTERISTICS['11'],
    'bb' : BAND_CHARACTERISTICS['12'],
    'bc' : BAND_CHARACTERISTICS['13'],
    'bd' : BAND_CHARACTERISTICS['14'],
    'be' : BAND_CHARACTERISTICS['15'],
    'bf' : BAND_CHARACTERISTICS['16'],

    // full disk
    '90' : BAND_CHARACTERISTICS['1'],
    '91' : BAND_CHARACTERISTICS['2'],
    '92' : BAND_CHARACTERISTICS['3'],
    '93' : BAND_CHARACTERISTICS['4'],
    '94' : BAND_CHARACTERISTICS['5'],
    '95' : BAND_CHARACTERISTICS['6'],
    '96' : BAND_CHARACTERISTICS['7'],
    '97' : BAND_CHARACTERISTICS['8'],
    '98' : BAND_CHARACTERISTICS['9'],
    '99' : BAND_CHARACTERISTICS['10'],
    '9a' : BAND_CHARACTERISTICS['11'],
    '9b' : BAND_CHARACTERISTICS['12'],
    '9c' : BAND_CHARACTERISTICS['13'],
    '9d' : BAND_CHARACTERISTICS['14'],
    '9e' : BAND_CHARACTERISTICS['15'],
    '9f' : BAND_CHARACTERISTICS['16'],

    // mesoscale
    'e0' : BAND_CHARACTERISTICS['1'],
    'e1' : BAND_CHARACTERISTICS['2'],
    'e2' : BAND_CHARACTERISTICS['3'],
    'e3' : BAND_CHARACTERISTICS['4'],
    'e4' : BAND_CHARACTERISTICS['5'],
    'e5' : BAND_CHARACTERISTICS['6'],
    'e6' : BAND_CHARACTERISTICS['7'],
    'e7' : BAND_CHARACTERISTICS['8'],
    'e8' : BAND_CHARACTERISTICS['9'],
    'e9' : BAND_CHARACTERISTICS['10'],
    'ea' : BAND_CHARACTERISTICS['11'],
    'eb' : BAND_CHARACTERISTICS['12'],
    'ec' : BAND_CHARACTERISTICS['13'],
    'ed' : BAND_CHARACTERISTICS['14'],
    'ee' : BAND_CHARACTERISTICS['15'],
    'ef' : BAND_CHARACTERISTICS['16'],

    'c0' : BAND_CHARACTERISTICS['1'],
    'c1' : BAND_CHARACTERISTICS['2'],
    'c2' : BAND_CHARACTERISTICS['3'],
    'c3' : BAND_CHARACTERISTICS['4'],
    'c4' : BAND_CHARACTERISTICS['5'],
    'c5' : BAND_CHARACTERISTICS['6'],
    'c6' : BAND_CHARACTERISTICS['7'],
    'c7' : BAND_CHARACTERISTICS['8'],
    'c8' : BAND_CHARACTERISTICS['9'],
    'c9' : BAND_CHARACTERISTICS['10'],
    'ca' : BAND_CHARACTERISTICS['11'],
    'cb' : BAND_CHARACTERISTICS['12'],
    'cc' : BAND_CHARACTERISTICS['13'],
    'cd' : BAND_CHARACTERISTICS['14'],
    'ce' : BAND_CHARACTERISTICS['15'],
    'cf' : BAND_CHARACTERISTICS['16'],

    'f0' : BAND_CHARACTERISTICS['1'],
    'f1' : BAND_CHARACTERISTICS['2'],
    'f2' : BAND_CHARACTERISTICS['3'],
    'f3' : BAND_CHARACTERISTICS['4'],
    'f4' : BAND_CHARACTERISTICS['5'],
    'f5' : BAND_CHARACTERISTICS['6'],
    'f6' : BAND_CHARACTERISTICS['7'],
    'f7' : BAND_CHARACTERISTICS['8'],
    'f8' : BAND_CHARACTERISTICS['9'],
    'f9' : BAND_CHARACTERISTICS['10'],
    'fa' : BAND_CHARACTERISTICS['11'],
    'fb' : BAND_CHARACTERISTICS['12'],
    'fc' : BAND_CHARACTERISTICS['13'],
    'fd' : BAND_CHARACTERISTICS['14'],
    'fe' : BAND_CHARACTERISTICS['15'],
    'ff' : BAND_CHARACTERISTICS['16']
  },

  caBlocks : {
    92 : ['1314-6328']
  }
}

module.exports = config;