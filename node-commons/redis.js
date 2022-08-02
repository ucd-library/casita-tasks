import Redis from 'ioredis';
import config from './config.js';
import logger from './logger.js';
import waitUntil from './wait-until.js';

class RedisClient {

  async _initClient() {
    await waitUntil(config.redis.host, config.redis.port);

    this.client = new Redis({
      host: config.redis.host,
      port : config.redis.port
    });

    this.client.on('error', (err) => {
      logger.error('Redis client error', err);
    });
    this.client.on('ready', () => {
      logger.info('Redis client ready');
    });
    this.client.on('end', () => {
      logger.info('Redis client closed connection');
    });
    this.client.on('reconnecting', () => {
      logger.info('Redis client reconnecting');
    });
  }

  /**
   * @method connect
   * @description create/connect redis client
   */
  connect() {
    if( !this.client ) {
      return this._initClient();
    }
  }

  /**
   * @method disconnect
   * @description disconnect redis client
   * 
   * @returns {Promise}
   */
  disconnect() {
    return new Promise((resolve, reject) => {
      this.client.quit(() => resolve());
    });
  }

}

let instance = new RedisClient();
export default instance;