const child_process = require('child_process');
const path = require('path');
const {URL} = require('url');
const config = require('./config');
const { stderr } = require('process');

module.exports = function scale(file, band=1) {
  let info = config.bandResolutions[parseInt(band)];
  let scale = (info.resolution * 25);

  let fileParsed = path.parse(file);
  let outfile = path.join(fileParsed.dir, 'web-scaled.png');
  file = path.join(fileParsed.dir, 'web.png');

  return new Promise((resolve, reject) => {
    child_process.exec(
      `convert ${file} -resize %${scale} ${outfile}`, 
      {shell: '/bin/bash'},
      (error, stdout, stderr) => {
        if( error ) reject(error);
        else resolve({stdout, stderr});
      }
    )
  });
}