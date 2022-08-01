import dot from 'dot-object';
import path from 'path';
import merge from 'deepmerge';
import fs from 'fs';
const env = process.env;

const __dirname = path.parse(import.meta.url.replace('file://', '')).dir;

let dotPathMap = {
  'debugConfig' : 'debugConfig',
  'quiet': 'logging.quiet',
  'directory' : 'directory',
  'file' : 'file',
  'metadataFile' : 'metadataFile',
  'metrics' : 'google.metrics',
  'googleApplicationCredentials' : 'google.applicationCredentials',
  'googleProjectId' : 'google.projectId',
  'command' : 'command.current',
  'commandRef' : 'command.reference',
  'kafkaHost' : 'kafka.host',
  'kafkaPort' : 'kafka.port',
  'kafka' : 'kafka.topic',
  'kafkaExternal' : 'kafka.external',
  'printKafkaMsg' : 'kafka.print'
};

let credentialProjectId;
if( env.GOOGLE_APPLICATION_CREDENTIALS ) {
  let content = JSON.parse(fs.readFileSync(env.GOOGLE_APPLICATION_CREDENTIALS, 'utf-8'));
  credentialProjectId = content.project_id;
}

function handlePortEnv(value) {
  if( value && value.match(':') ) {
    return value.split(':').pop();
  }
  return value;
}

// k8s inserts a kafka port like tcp://10.109.128.0:9092.  clean up
let kafkaPort = handlePortEnv(env.KAFKA_PORT);
let rabbitMqPort = handlePortEnv(env.RABBITMQ_PORT);


let config = {
  instance : env.INSTANCE_ENV || 'sandbox',
  satellite : process.env.SATELLITE || 'west',

  streams : ['decoded', 'secdecoded'],

  google : {
    metrics : false,
    applicationCredentials : env.GOOGLE_APPLICATION_CREDENTIALS || '',
    projectId : env.GOOGLE_PROJECT_ID || credentialProjectId ||  ''
  },

  command : {
    map : {
      image : 'node-image-utils',
      generic : 'generic-payload-parser'
    },
    current : '',
    reference : ''
  },

  fs : {
    nfsRoot : env.NFS_ROOT || '/storage/network'
  },
  
  kafka : {
    clientId : env.KAFKA_CLIENT_ID || 'default-casita-client',
    port : kafkaPort || 9092,
    host : env.KAFKA_HOST || 'kafka',
    // TODO: if you update a topic name, make sure you do it in services/init/kafka.js as well
    topics : {
      decoder : 'goes-decoder',
      productWriter : 'goes-nfs-product',
      tasks : 'tasks',
      blockCompositeImage : 'block-composite-image',
      ringBuffer : 'ring-buffer',
      lightning : 'lightning',
      lightningGroupStats : 'lightning-grouped-stats'
    },
    groups : {
      productWriter : env.KAFKA_PRODUCT_WRITER_GROUP_ID || 'product-writer',
      worker : env.KAFKA_CASITA_WORKER_GROUP_ID || 'casita-worker',
      external : env.KAFKA_EXTERNAL_GROUP_ID || 'external'
    }
  },

  logging : {
    quite : (env.LOG_QUITE === 'true'),
    name : env.LOG_NAME || 'casita-worker',
    level : env.LOG_LEVEL || 'info'
  },

  pg : {
    host : env.PG_HOST || 'postgres-service',
    user : env.PG_USERNAME || 'postgres',
    port : env.PG_PORT || 5432,
    database : env.PG_DATABASE || 'casita',

    ringBuffer : {
      table : 'public.blocks_ring_buffer',
      size : 10, // days,
      preloadTablePrefix : 'raster'
    }
  },

  rabbitMq : {
    host : env.RABBITMQ_HOST || 'rabbitmq-service',
    port : parseInt(rabbitMqPort || 5672),
    defaultPriority : 10,
    queues : {
      tasks : 'tasks'
    },
    config : {
      heartbeat : 60*30
    }
  },

  // remove files from nfs
  expire : {
    direction : env.EXPIRE_DIRECTION || 'forward',
    cron : '0 0 * * *', // at every hour
    minDepth : 3,
    default : {
      maxDepth : parseInt(env.EXPIRE_DIR_DEPTH || 4),
      expireTime : 24
    },
    custom : {
      california : {
        expireTime : 24 * 31,
        regex : /\/west\/ca-[a-z]+\/.+/,
        maxDepth : 3
      }
    }
  },

  // external api routes that are proxied by
  // main rest service
  rest : {
    proxyServices : [{
      route : '/ws',
      hostname : 'external-topics-service'
    }]
  }
}

export default config
function update(args, cmd) {
  let parent = cmd.parent ? cmd.parent.name() : '';
  args.command = 'tasks/nodejs/'+((parent ? parent+'/' : '') + cmd.name()).replace(/casita-/g, '');
  args.commandRef = getCommandReference(args.command);

  if( args.kafkaPort ) {
    args.kafkaPort = handlePortEnv(args.kafkaPort);
  }
  
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