import {exec} from 'child_process';

function _exec (cmd, opts={}) {
  if( !opts.shell ) opts.shell = '/bin/bash';
  if( !opts.maxBuffer ) opts.maxBuffer =  1024 * 1000 * 10;
  
  return new Promise((resolve, reject) => {
    exec(cmd, opts, (error, stdout, stderr) => {
      if( error ) reject(error);
      else resolve({stdout, stderr});
    });
  });
}

export default _exec;