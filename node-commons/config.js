import dot from 'dot-object';
import path from 'path';
import merge from 'deepmerge';
import fs from 'fs';
const env = process.env;

const __dirname = path.parse(import.meta.url.replace('file://', '')).dir;

let dotPathMap = {
  'quiet': 'logging.quiet',
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
let redisPort = handlePortEnv(env.REDIS_PORT);

let DEFAULT_CA_EXPIRE = 24 * 30; // 30 days

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
      caProjection : 'ca-projection',
      caProjectionHourlyStats : 'ca-projection-hourly-stats',
      decoder : 'goes-decoder',
      productWriter : 'goes-nfs-product',
      tasks : 'tasks',
      blockCompositeImage : 'block-composite-image',
      ringBuffer : 'ring-buffer',
      thermalAnomaly : 'thermal-anomaly',
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
    quiet : (env.LOG_QUIET === 'true'),
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
      // by band, in days
      defaultSize : 0.25, // day
      sizes : [
        {key : 'band', value: 1, size : 0.5},
        {key : 'band', value: 2, size : 0.5},
        {key : 'band', value: 7, size : 0.5},
        {key : 'product', value: 'california', size : 10}
      ],
      preloadTablePrefix : 'raster'
    },

    roi : {
      bufferTable : 'roi.roi_buffer',
      products : {
        california3310 : {
          id : 'ca',
          product : 'california',
          apid : 'imagery'
        },
        californiaGoes : {
          id : 'ca-goes-west',
          product : 'california',
          apid : 'imagery'
        } 
      }
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

  redis : {
    host : env.REDIS_HOST || 'redis-master',
    port : redisPort || '6379'
  },

  fsCache : {
    expire : 60 * 30 // 30 min
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
        expireTime : parseInt(env.CA_PRODUCT_EXPIRE || DEFAULT_CA_EXPIRE),
        regex : /\/west\/california\//,
        maxDepth : 3
      },
      thermalAnomaly : {
        expireTime : parseInt(env.CA_PRODUCT_EXPIRE || DEFAULT_CA_EXPIRE),
        regex : /\/west\/thermal-anomaly\//,
        maxDepth : 3
      }
    }
  },

  thermalAnomaly : {
    eventRadius : 5,
    stddevClassifier : 4,
    product : 'thermal-anomaly',
    products : ['hourly-max', 'hourly-max-10d-average', 'hourly-max-10d-stddev',
      'hourly-max-10d-max', 'hourly-max-10d-min'],
    slack : {
      urlSecret : 'slack-goesr-thermal-event-webook'
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
  // TODO: can we itegrate this with module runner?
  if( cmd ) {
    let parent = cmd.parent ? cmd.parent.name() : '';
    args.command = 'tasks/nodejs/'+((parent ? parent+'/' : '') + cmd.name()).replace(/casita-/g, '');
    args.commandRef = getCommandReference(args.command);
  }

  if( args.kafkaPort ) {
    args.kafkaPort = handlePortEnv(args.kafkaPort);
  }
  
  Object.assign(
    config, 
    merge(config, dot.transform(dotPathMap, args))
  );

  // check for root properties not defined in map
  for( let key in args ) {
    if( dotPathMap[key] ) continue;
    config[key] = args[key];
  }

};
export {update}

function getCommandReference(cmd) {
  return path.resolve(__dirname, '..', cmd.split('/')
    .map(part => config.command.map[part] ? config.command.map[part] : part)
    .join('/'))+'.js'
}