const {exec} = require('child_process');

module.exports = (cmd, opts={}) => {
  // opts.shell = '/bin/bash';
  opts.maxBuffer =  1024 * 1000 * 10;
  return new Promise((resolve, reject) => {
    exec(cmd, opts, (error, stdout, stderr) => {
      if( error ) reject(error);
      else resolve({stdout, stderr});
    });
  });
}