const {Client} = require('pg');
const path = require('path');
const fs = require('fs');

class PG {

  constructor() {
    this.client = new Client({
      host : process.env.PG_HOST || 'postgres',
      user : process.env.PG_USERNAME || 'postgres',
      port : process.env.PG_PORT || 5432,
      database : process.env.PG_DATABASE || 'casita'
    });
  }

  async connect() {
    if( this.connected ) return;

    if( this.connecting ) {
      await this.connecting;
    } else {
      this.connecting = this.client.connect();
      await this.connecting;
      this.connecting = null;
      this.connected = true;
    }
  }

  async disconnect() {
    if( !this.connected ) return;
    await this.client.disconnect();
    this.connected = false;
  }

  async query(query, params) {
    await this.connect();
    return this.client.query(query, params);
  }
}

module.exports = new PG();