const fs = require('fs-extra');
const path = require('path');
const Worker = require('../service/lib/worker');
const {state, config} = require('@ucd-lib/krm-node-utils');
const monitoring = require('./lib/monitoring');

const mongo = state.mongo;
const ObjectId = mongo.ObjectId;

const COLLECTION = 'stream-status';
const EXPIRE = 1000 * 60 * 60 * 24 * 7; // one week

class StatusWorker extends Worker {

  constructor() {
    super();

    this.ensureIndexes();
    setInterval(() => this.cleanup(), 1000 * 60 * 60 * 1);
  }

  async exec(msg) {
    if( msg.data.command.type === 'time_to_disk' ) {
      return this.timeToDisk(msg);
    } else if( msg.data.command.type === 'comp_png_gen_time' ) {
      return this.compPngGenTime(msg);
    }

    return {
      stdout : '',
      stderr : 'Unknown command type: '+msg.data.command.type
    }
  }

  async compPngGenTime(msg) {
    let pngFile = path.join(config.fs.nfsRoot, msg.data.command.file);
    let dir = path.parse(pngFile).dir;
    let metadataFile = path.join(dir, 'fragment-metadata.json');

    let [satellite, scale, date, hour, minsec, band, apid, blocks, block] = msg.data.command.file.replace(/^\//, '').split('/');
    let pngTime = (await fs.stat(pngFile)).ctime;
    let mTime = (await fs.stat(metadataFile)).ctime;

    monitoring.addCPGT(
      apid, 
      pngTime.getTime() - mTime.getTime(), 
      new Date(), 
      block
    );

    return {stdout : `success: ${data.apid}, ${block}`, stderr:''};
  }

  async timeToDisk(msg) {
    let metadataFile = path.join(config.fs.nfsRoot, msg.data.command.file);
    let data = await this.readJsonFile(metadataFile, 'utf-8');

    // let serverTime = new Date(msg.time);
    let serverTime = fs.statSync(metadataFile).ctime;

    let captureTime = null;
    if( data.type === 'generic' ) {
      if( data.headers && data.headers.SECONDS_SINCE_EPOCH ) {
        captureTime = new Date(946728000000 + data.headers.SECONDS_SINCE_EPOCH*1000);
      }
    } else if( data.type === 'image' ) {
      if( data && 
          data.imagePayload && 
          data.imagePayload.SECONDS_SINCE_EPOCH ) {
        captureTime = new Date(946728000000 + data.imagePayload.SECONDS_SINCE_EPOCH*1000);
      }
    }

    let statusUpdate = {
      [`apid.${data.apid}`]: { serverTime, captureTime },
      [`stream.${data.streamName}`] : { serverTime, captureTime }
    };

    let collection = await mongo.getCollection(COLLECTION);
    let stdout = '';
    let stderr = '';


    monitoring.addTTD(data.apid, serverTime.getTime() - captureTime.getTime(), serverTime, data.streamName);

    try {
      await collection.insert({
        apid : data.apid,
        stream : data.streamName,
        serverTime, captureTime,
        file:  msg.data.command,
        diff : serverTime.getTime() - captureTime.getTime()
      });

      await collection.updateOne(
        { _id : 'overview' },
        { $set: statusUpdate  },
        { 
          returnOriginal: false,
          upsert: true
        }
      );
      stdout = `success: ${data.streamName}, ${data.apid}`;
    } catch(e) {
      stderr = `failed: ${data.streamName}, ${data.apid}: ${e.message}`;
    }

    return {stdout, stderr};
  }

  async readJsonFile(file) {
    return JSON.parse(await fs.readFile(file, 'utf-8'));
  }

  async cleanup() {
    let collection = await mongo.getCollection(COLLECTION);
    let d = new Date();
    d.setTime(d.getTime() - EXPIRE);

    await collection.remove({
      serverTime : {$lt : d}
    });
  }

  async ensureIndexes() {
    let collection = await mongo.getCollection(COLLECTION);
    collection.createIndex({stream: 1});
    collection.createIndex({apid: 1});
    collection.createIndex({serverTime: 1});
    collection.createIndex({captureTime: 1});
  }


}

let worker = new StatusWorker();
worker.connect();