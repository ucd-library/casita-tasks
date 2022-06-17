import bunyan from 'bunyan';
import {LoggingBunyan} from '@google-cloud/logging-bunyan';
import config from './config.js';

const streams = [];

console.log(config.logging.quiet);
if( config.logging.quiet !== true ) {
  streams.push({ stream: process.stdout });
}

// wire in stack driver if google cloud service account provided
if( config.google.applicationCredentials ) {

  // create bunyan logger for stackdriver
  let loggingBunyan = new LoggingBunyan({
    projectId: config.google.projectId,
    keyFilename: config.google.applicationCredentials,
    // enables error reporting on error
    // serviceContext: {
    //   service: config.instance.name,
    //   version: config.instance.version
    // }
  });

  // add new logger stream
  streams.push(loggingBunyan.stream(config.logging.level));
}

let logger = bunyan.createLogger({
  name: config.logging.name,
  level: config.logging.level,
  streams: streams
});

let info = {
  name: config.logging.name,
  level: config.logging.level,
  gcLogging : {
    enabled : config.google.applicationCredentials ? true : false,
    file : config.google.applicationCredentials
  }
}

logger.info('logger initialized', info);

export default logger;