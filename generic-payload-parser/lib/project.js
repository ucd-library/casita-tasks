const {exec} = require('child_process');

// const CMD = 'proj +proj=geos +x_0=0 +y_0=0 +lon_0=-75 +sweep=x +h=35786023 +ellps=GRS80 +datum=NAD83 +units=m +no_defs';
const CMD = 'proj +proj=geos +x_0=0 +y_0=0 +lon_0=-137 +sweep=x +h=35786023 +ellps=GRS80 +datum=NAD83 +units=m +no_defs';

const FOV_OFFSET_LNG = -75;
const SIZE = 21696;
const H_SIZE = SIZE/2;

module.exports = (lng, lat) => {
  // lng = FOV_OFFSET_LNG + lng;

  return new Promise((resolve, reject) => {
    let child = exec(CMD, {shell: '/bin/bash'}, (error, stdout, stderr) => {
      if( error ) {
        reject(error);
      } else {
        stdout = stdout
          .trim()
          .split(/\t/)
          .map(v => parseFloat(v)/ 500);

        if( stdout.length > 1 ) {
          stdout[0] += H_SIZE;
          if( stdout[1] > 0 ) stdout[1] = H_SIZE - stdout[1];
          else stdout[1] = (-1 * stdout[1]) + H_SIZE;
        }
        stdout = stdout.map(v => Math.floor(v));
        resolve({x: stdout[0], y:stdout[1]});
      }
    });
    child.stdin.write(lng.toFixed(5)+' '+lat.toFixed(5));
    child.stdin.end();
  });
}