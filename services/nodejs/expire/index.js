import {logger, config} from '@ucd-lib/casita-worker';
import {CronJob} from 'cron';
import fs from 'fs-extra';
import path from 'path';

const DIRECTION = config.expire.direction;
const MAX_DEPTH = config.expire.maxDepth;

class Expire {

  constructor(path) {
    this.path = path || config.fs.nfsRoot;
    this.expireCron = new CronJob(config.cron.fsExpire, () => this.run());
    this.expireCron.start();
    this.run();
  }

  async run() {
    if( this.running ) return;

    this.running = true;
    logger.info(`Starting ${DIRECTION} expire process for ${this.path}.  MAX_DEPTH=${MAX_DEPTH}`);
    await this.expire(this.path);
    logger.info(`Completed ${DIRECTION} expire process for ${this.path}`);
    this.running = false;
  }

  async expire(folder, depth=-1) {
    let cdepth = depth + 1;
    let files;

    logger.debug(`Crawlng ${folder} depth=${depth}`)
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
    proot = file.replace(config.fs.nfsRoot, '');

    try {
      stat = fs.lstatSync(file);
    } catch(e) {
      return;
    }

    let age = Date.now() - stat.mtime.getTime();

    // if directory
    if( stat.isDirectory() ) {

      // check custom

      for( let key in config.expire.custom ) {
        let def = config.expire.custom[key];
        if( age > def.expireTime * 1000 * 60 * 60 ) {
          logger.debug(`expire dir (custom=${key}): `+file);
          try {
            await fs.remove(file);
          } catch(e) {}
          return;
        }
      }

      // if beyond max depth stop crawling
      if( MAX_DEPTH !== -1 && depth >= MAX_DEPTH ) {
        // if dir is expired, remove it and all files
        if( age > config.expire.default.expireTime * 1000 * 60 * 60) {
          logger.debug(`expire dir (depth=${depth}): `+file);
          try {
            await fs.remove(file);
          } catch(e) {}
        } else {
          await this.expire(file, depth);
        }
      
        return;
      }

      if( age > config.fs.expire * 1000 ) {
        try {
          logger.debug('expire: '+file);
          await fs.remove(file, depth);
        } catch(e) {}
      }
    }
  }

}

new Expire();