const {Client} = require('pg');
const path = require('path');
const fs = require('fs');
const {logger} = require('@ucd-lib/krm-node-utils');


class PG {

  constructor() {
    this.client = new Client({
      host : process.env.PG_HOST || 'postgres',
      user : process.env.PG_USERNAME || 'postgres',
      port : process.env.PG_PORT || 5432,
      database : process.env.PG_DATABASE || 'casita'
    });

    this.client.on('end', async () => {
      logger.info('Postgresql client end event');
      await this.reconnect();
    });
    this.client.on('error', async e => {
      logger.info('Postgresql client error event', e);
      await this.reconnect();
    });
  }

  async reconnect() {
    try { 
      await this.disconnect();
    } catch(e) { 
      logger.error('disconnect failed in reconnect()', e)
    }
    try { 
      await this.connect();
    } catch(e) { 
      logger.error('connect failed in reconnect()', e)
    }
  }

  async connect() {
    if( this.connected ) return;

    if( this.connecting ) {
      await this.connecting;
    } else {
      logger.info('Connecting to postgresql');
      this.connecting = this.client.connect();
      await this.connecting;
      logger.info('Connected to postgresql');
      this.connecting = null;
      this.connected = true;
    }
  }

  async disconnect() {
    await this.client.disconnect();
    this.connected = false;
    this.connecting = null;
  }

  async query(query, params) {
    await this.connect();
    return this.client.query(query, params);
  }
}

module.exports = new PG();