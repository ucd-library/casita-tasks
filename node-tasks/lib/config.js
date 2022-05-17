const dot = require('dot-object');
const path = require('path');
const merge = require('deepmerge');
const env = process.env;

let dotPathMap = {
  'debugConfig' : 'debugConfig',
  'quiet': 'logging.quiet',
  'directory' : 'directory',
  'file' : 'file',
  'metrics' : 'google.metrics',
  'googleApplicationCredentials' : 'google.applicationCredentials',
  'googleProjectId' : 'google.projectId',
  'command' : 'command.current',
  'commandRef' : 'command.reference',
  'kafkaHost' : 'kafka.host',
  'kafkaPort' : 'kafka.port',
  'kafkaTopic' : 'kafka.topic',
  'printKafkaMsg' : 'kafka.print'
};

let config = {
  instance : env.INSTANCE_NAME || 'sandbox',

  google : {
    metrics : false,
    applicationCredentials : env.GOOGLE_APPLICATION_CREDENTIALS || '',
    projectId : env.GOOGLE_PROJECT_ID || ''
  },

  command : {
    map : {
      image : 'node-image-utils'
    },
    current : '',
    reference : ''
  },
  
  kafka : {
    enabled : false,
    port : env.KAFKA_PORT || 9092,
    host : env.KAFKA_HOST || 'kafka',
    topic : env.KAFKA_TOPIC || ''
  },

  logging : {
    quite : (env.LOG_QUITE === 'true'),
    name : env.LOG_NAME || 'casita-worker',
    level : env.LOG_LEVEL || 'info'
  }
}

module.exports = config
module.exports.update = function(args, cmd) {
  let parent = cmd.parent ? cmd.parent.name() : '';
  args.command = ((parent ? parent+'/' : '') + cmd.name()).replace(/casita-/g, '');
  args.commandRef = getCommandReference(args.command);
  Object.assign(
    config, 
    merge(config, dot.transform(dotPathMap, args))
  );
};

function getCommandReference(cmd) {
  return path.resolve(__dirname, '..', cmd.split('/')
    .map(part => config.command.map[part] ? config.command.map[part] : part)
    .join('/'))+'.js'
}