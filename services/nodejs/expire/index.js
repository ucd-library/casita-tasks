import {logger, config, exec} from '@ucd-lib/casita-worker';
import {CronJob} from 'cron';
import fs from 'fs-extra';
import path from 'path';

const DIRECTION = config.expire.direction;
const MAX_DEPTH = config.expire.default.maxDepth;

class Expire {

  constructor(path) {
    this.path = path || config.fs.nfsRoot;
    this.expireCron = new CronJob(config.expire.cron, () => this.run());
    this.expireCron.start();
    this.run();
  }

  async run() {
    if( this.running ) return;

    this.running = true;
    logger.info(`Starting ${DIRECTION} expire process for ${this.path}.  MAX_DEPTH=${MAX_DEPTH}`);
    await this.expire(this.path, 0);
    logger.info(`Completed ${DIRECTION} expire process for ${this.path}`);
    this.running = false;
  }

  async expire(folder, depth=-1) {
    let cdepth = depth + 1;
    let files;

    logger.debug(`Crawling ${folder} depth=${depth}`)
    try {
      files = await fs.readdir(folder);
    } catch(e) {
      logger.warn('Failed to read directory: '+folder);
      return;
    }    

    if( DIRECTION === 'forward' ) {
      for( let i = 0; i < files.length; i++ ) {
        await this.removeFile(folder, files[i], cdepth);
      }
    } else {
      for( let i = files.length-1; i >= 0; i-- ) {
        await this.removeFile(folder, files[i], cdepth);
      }
    }

    try {
      files = await fs.readdir(folder);
    } catch(e) {
      logger.debug('Failed to read directory: '+folder);
      return;
    }

    if( files.length === 0 ) {
      try {
        await fs.remove(folder);
      } catch(e) {}
    }
  }

  async removeFile(folder, file, depth) {
    let stat;
    file = path.join(folder, file);
    let proot = file.replace(config.fs.nfsRoot, '');


    if( depth < config.expire.minDepth ) {
      logger.debug(`Ignoring ${file}, depth ${depth} less than minDepth of ${config.expire.minDepth}`);
      await this.expire(file, depth);
      return;
    }

    try {
      stat = fs.lstatSync(file);
    } catch(e) {
      logger.warn('failed to lstat file: '+file, e);
      return;
    }

    let age = Date.now() - stat.mtime.getTime();

    // check custom
    for( let key in config.expire.custom ) {
      let def = config.expire.custom[key];

      if( proot.match(def.regex) ) {
        if( age > def.expireTime * 1000 * 60 * 60 ) {
          logger.debug(`expire dir (custom=${key}): `+file);
          await this.remove(file);
        } else if( depth >= (def.maxDepth || MAX_DEPTH) ) {
          return; // stop looking
        } else {
          await this.expire(file, depth);
        }
        return;
      }
    }

    // if beyond max depth stop crawling
    if( MAX_DEPTH !== -1 && depth >= MAX_DEPTH ) {
      // if dir is expired, remove it and all files
      if( age > config.expire.default.expireTime * 1000 * 60 * 60) {
        logger.debug(`expire dir (depth=${depth}): `+file);
        await this.remove(file);
      }
      return;
    }

    if( age > config.expire.default.expireTime * 1000 ) {
      logger.debug(`expire (depth=${depth}): `+file);
      await this.remove(file);
      return;
    }

    // if directory
    if( stat.isDirectory() ) {
      await this.expire(file, depth);
    }
    
  }

  /**
   * @method remove
   * @description remove file, this will capture errors.  Trys rmdir first
   * and rm -rf second.
   * 
   * @param {String} file File to remove 
   */
  async remove(file) {
    try {
      await fs.remove(file);
    } catch(e) {
      await this.removeExec(file);
    }
  }

  /**
   * @method removeExec
   * @description fs.remove calls rmdir under the hood which fails in minikube.
   * This is a general backup call for remove.
   * 
   * @param {String} file file to remove 
   */
  async removeExec(file) {
    try {
      await exec(`rm -rf ${file}`);
    } catch(e) {
      logger.error('Failed to expire file: '+file, e);
    }
  }

}

new Expire();