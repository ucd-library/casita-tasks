import dot from 'dot-object';
import path from 'path';
import merge from 'deepmerge';
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

// k8s inserts a kafka port like tcp://10.109.128.0:9092.  clean up
let kafkaPort = env.KAFKA_PORT;
if( kafkaPort && kafkaPort.match(/:/) ) {
  kafkaPort = kafkaPort.split(':').pop();
}

let config = {
  instance : env.INSTANCE_ENV || 'sandbox',

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

  fs : {
    nfsRoot : env.NFS_ROOT || '/storage/network'
  },
  
  kafka : {
    enabled : false,
    port : kafkaPort || 9092,
    host : env.KAFKA_HOST || 'kafka',
    topics : {
      decoder : env.KAFKA_DECODER_TOPIC || 'goes-decoder',
      productWriter : env.KAFKA_PRODUCT_WRITER_TOPIC || 'goes-nfs-product',
    },
    groups : {
      productWriter : env.KAFKA_PRODUCT_WRITER_GROUP_ID || 'product-writer'
    }
  },

  logging : {
    quite : (env.LOG_QUITE === 'true'),
    name : env.LOG_NAME || 'casita-worker',
    level : env.LOG_LEVEL || 'info'
  }
}

export default config
function update(args, cmd) {
  let parent = cmd.parent ? cmd.parent.name() : '';
  args.command = 'nodejs/'+((parent ? parent+'/' : '') + cmd.name()).replace(/casita-/g, '');
  args.commandRef = getCommandReference(args.command);
  Object.assign(
    config, 
    merge(config, dot.transform(dotPathMap, args))
  );
};
export {update}

function getCommandReference(cmd) {
  return path.resolve(__dirname, '..', cmd.split('/')
    .map(part => config.command.map[part] ? config.command.map[part] : part)
    .join('/'))+'.js'
}