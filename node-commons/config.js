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
  'kafkaTopic' : 'kafka.topic',
  'printKafkaMsg' : 'kafka.print'
};

let credentialProjectId;
if( env.GOOGLE_APPLICATION_CREDENTIALS ) {
  let content = fs.readFileSync(env.GOOGLE_APPLICATION_CREDENTIALS);
  credentialProjectId = content.project_id;
}

// k8s inserts a kafka port like tcp://10.109.128.0:9092.  clean up
let kafkaPort = env.KAFKA_PORT;
if( kafkaPort && kafkaPort.match(/:/) ) {
  kafkaPort = kafkaPort.split(':').pop();
}

const AIRFLOW_HOST = env.AIRFLOW_HOST || 'airflow-webserver:8080';

let config = {
  instance : env.INSTANCE_ENV || 'sandbox',
  satellite : process.env.SATELLITE || 'west',

  google : {
    metrics : false,
    applicationCredentials : env.GOOGLE_APPLICATION_CREDENTIALS || '',
    projectId : env.GOOGLE_PROJECT_ID || credentialProjectId ||  ''
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
    clientId : env.KAFKA_CLIENT_ID || 'default-casita-client',
    port : kafkaPort || 9092,
    host : env.KAFKA_HOST || 'kafka',
    // TODO: if you update a topic name, make sure you do it in services/init/kafka.js as well
    topics : {
      decoder : 'goes-decoder',
      productWriter : 'goes-nfs-product',
      tasks : 'tasks',
      blockCompositeImage : 'block-composite-image'
    },
    groups : {
      productWriter : env.KAFKA_PRODUCT_WRITER_GROUP_ID || 'product-writer',
      worker : env.KAFKA_CASITA_WORKER_GROUP_ID || 'casita-worker'
    }
  },

  logging : {
    quite : (env.LOG_QUITE === 'true'),
    name : env.LOG_NAME || 'casita-worker',
    level : env.LOG_LEVEL || 'info'
  },

  airflow : {
    host : AIRFLOW_HOST,
    baseApi : `http://${AIRFLOW_HOST}/api/v1/dags`,
    username : env._AIRFLOW_WWW_USER_USERNAME || 'airflow',
    password : env._AIRFLOW_WWW_USER_PASSWORD || 'airflow'
  }
}

export default config
function update(args, cmd) {
  let parent = cmd.parent ? cmd.parent.name() : '';
  args.command = 'tasks/nodejs/'+((parent ? parent+'/' : '') + cmd.name()).replace(/casita-/g, '');
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