import {exec} from 'child_process';
import {logger} from '@ucd-lib/casita-worker';

function execMessage(message) {
  let opts = message.opts || {};
  if( !opts.shell ) opts.shell = '/bin/bash';

  logger.debug(`Exec following command/opts: ${message.cmd}`, opts);

  return new Promise((resolve, reject) => {
    let child = exec(message.cmd, opts, (error, stdout, stderr) => {
      if( error ) {
        reject(error);
      } else {
        logger.debug(`Exec complete command/opts: ${message.cmd}`, {stdout, stderr});
        resolve({stdout, stderr, exitCode : child.exitCode});
      }
    });
  });
}

export default execMessage;