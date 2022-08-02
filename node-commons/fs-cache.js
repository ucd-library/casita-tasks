import redis from './redis.js';
import config from './config.js';
import fs from 'fs-extra';

const REDIS_PREFIX = 'fs-cache-';

class FsCache {

  connect() {
    return redis.connect();
  }

  getKey(file) {
    return REDIS_PREFIX+file;
  }

  async set(file, contents) {
    await this.connect();

    if( !contents ) {
      contents = await fs.readFile(file, 'utf-8');
    }

    let key = this.getKey(file);
    await redis.client.set(key, contents);
    await redis.client.expire(key, config.fsCache.expire);

    return contents;
  }

  async get(file) {
    await this.connect();
    let key = this.getKey(file);
    let contents = await redis.client.get(key);
    if( !contents ) contents = this.set(file);
    return contents;
  }

}

const instance = new FsCache();
export default instance;