import {exec} from 'child_process';
import {logger} from '@ucd-lib/casita-worker';

function execMessage(message) {
  let opts = message.opts || {};
  if( !opts.shell ) opts.shell = '/bin/bash';

  logger.debug(`Exec following command/opts: ${message.cmd}`, opts);

  return new Promise((resolve, reject) => {
    exec(message.cmd, opts, (error, stderr, stdout) => {
      if( error ) {
        reject(error);
      } else {
        logger.debug(`Exec complete command/opts: ${message.cmd}`, {stdout, stderr});
        resolve({stdout, stderr});
      }
    });
  });
}

export default execMessage;