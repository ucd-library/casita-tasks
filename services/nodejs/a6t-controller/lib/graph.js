// import kafkaWorker from './kafka.js';
import rabbitMqWorker from './rabbitmq.js';
import pathUtils from 'path';
import {config, fsCache} from '@ucd-lib/casita-worker';


const LIGHTNING_PAYLOAD_APIDS = /^(301|302)$/;

// const CASITA_CMD = 'casita';
const CASITA_CMD = 'node /casita/tasks/cli/casita.js';
const TOPICS = config.kafka.topics;

async function isBandReady(msgs) {
  let fragments = msgs.filter(item => item.data.file.base === 'image-fragment.jp2');
  let metadata = msgs.filter(item => item.data.file.base === 'fragment-metadata.json');

  if( !metadata.length ) return false;

  metadata = metadata[0].data.file;
  let file = pathUtils.join(metadata.dir, metadata.base);

  let info = await fsCache.get(file, true);
  info = JSON.parse(info);

  if ( info.fragmentsCount <= fragments.length ) {
    return true;
  }

  return false;
}

rabbitMqWorker.connect();


const dag = {

  // [config.kafka.topics.productWriter] : {
  //   source : true
  // },

  [TOPICS.blockCompositeImage] : {
    enabled: true,
    dependencies : [TOPICS.productWriter],

    where : msg => ['image-fragment.jp2', 'fragment-metadata.json'].includes(msg.data.file.base),
    groupBy : msg => `${msg.data.product}-${msg.data.date}T${msg.data.hour}:${msg.data.minsec}-${msg.data.x},${msg.data.y}-${msg.data.band}`,
    expire : 5, // 5 seconds
    ready : (key, msgs) => isBandReady(msgs),

    sink : (key, msgs) => {
      let task = TOPICS.blockCompositeImage;
      let {satellite, product, date, hour, minsec, file, band, apid, x, y} = msgs[0].data;

      let fmFile = pathUtils.join(config.fs.nfsRoot, satellite, product,
        date, hour, minsec, band, apid, 'blocks', x+'-'+y,
        'fragment-metadata.json');

      return rabbitMqWorker.exec({
          module : 'node-image-utils/jp2-to-png.js',
          args : {
            kafkaExternal : true,
            metadataFile : fmFile
          }
        },
        {task, msgs}
      );
      // return rabbitMqWorker.exec(`${CASITA_CMD} image jp2-to-png -p -e --metadata-file=${fmFile}`, {task, msgs});
    }
  },

  // no group by need here
  [TOPICS.ringBuffer] : {
    enabled: true,
    dependencies : [TOPICS.blockCompositeImage],
    where : msg => msg.data.band.match(/^(1|2|7)$/) ? true : false,
    sink : (key, msgs) => {
      let task = TOPICS.ringBuffer;
      let {satellite, product, date, hour, minsec, file, band, apid, x, y} = msgs[0].data;

      let pngFile = pathUtils.join(config.fs.nfsRoot, satellite, product,
        date, hour, minsec, band, apid, 'blocks', x+'-'+y,
        'image.png');

      return rabbitMqWorker.exec({
        module : 'block-ring-buffer/insert.js',
          args : {
            kafkaExternal : true,
            file : pngFile
          }
        },
        {task, msgs}
      );
      // return rabbitMqWorker.exec(`${CASITA_CMD} block-ring-buffer insert -p -e --file=${pngFile}`, {task, msgs});
    }
  },

  [TOPICS.ringBufferHourlyStats] : {
    enabled: true,
    dependencies : [TOPICS.ringBuffer],
    where : msg => {
      return msg.data.band === '7' && msg.data.x+'-'+msg.data.y === '1666-213' ? true : false
    },
    sink : (key, msgs) => {
      let task = TOPICS.ringBufferHourlyStats;
      let {blocks_ring_buffer_id} = msgs[0].data;

      return rabbitMqWorker.exec({
        module : 'block-ring-buffer/hourly-max-stats.js',
          args : {
            kafkaExternal : true,
            blocks_ring_buffer_id
          }
        }, 
        {task, msgs}
      );
      // return rabbitMqWorker.exec(`${CASITA_CMD} block-ring-buffer insert -p -e --file=${pngFile}`, {task, msgs});
    }
  },

  [TOPICS.thermalAnomaly] : {
    enabled: true,
    dependencies : [TOPICS.ringBufferHourlyStats],
    where :  msg => msg.data.band+'' === '7',
    sink : (key, msgs) => {
      let task = TOPICS.thermalAnomaly;
      let {blocks_ring_buffer_id} = msgs[0].data;

      return rabbitMqWorker.exec({
        module : 'thermal-anomaly/detection.js',
          args : {
            kafkaExternal : true,
            id: blocks_ring_buffer_id
          }
        }, 
        {task, msgs}
      );
      // return rabbitMqWorker.exec(`${CASITA_CMD} block-ring-buffer insert -p -e --file=${pngFile}`, {task, msgs});
    }
  },

  'ca-projection' : {
    enabled: true,
    dependencies : [config.kafka.topics.ringBuffer],
    groupBy : msg => `${msg.data.product}-${msg.data.date}T${msg.data.hour}:${msg.data.minsec}-${msg.data.band}`,
    where : msg => {
      return [
        '6664-852', '8332-852', '6664-1860','8332-1860', // conus b2
        '12656-2628', '14464-2628', '12656-3640', '14464-3640', // fulldisk b2
        '1666-213', '2083-213', '1666-465','2083-465', // conus b7
        '3164-657', '3616-657', '3164-910', '3616-910' // fulldisk b7
      ].includes(`${msg.data.x}-${msg.data.y}`);
    },
    ready : (key, msgs) => msgs.length === 4,

    sink : (key, msgs) => {
       console.log('\n\n\nca proj ready!', key);
      let {satellite, product, date, hour, minsec, file, band, apid, x, y} = msgs[0].data;
      return rabbitMqWorker.exec(`${CASITA_CMD} image ca-project -p -e --product=${product} --time=${date}T${hour}:${minsec}`, msgs);
    }
  },



  // 'full-composite-image' : {
  //   enabled : false,
  //   dependencies : ['block-composite-image'],

  //   groupBy : msg => `${msg.scale}-${msg.date}-${msg.hour}-${msg.minsec}-${msg.band}-${msg.apid}-${msg.file.base}`,
  //   where : data => ['image.png', 'web.png', 'web-scaled.png'].includes(data.file.base),
  //   expire : 60 * 20,

  //   ready : (key, msgs) => {
  //     let scale = msgs[0].scale;

  //     // TODO: wish there was a better way
  //     if( scale === 'mesoscale' && msgs.length >= 4 ) return true;
  //     if( scale === 'conus' && msgs.length >= 36 ) return true;
  //     if( scale === 'fulldisk' && msgs.length >= 229 ) return true;

  //     return false;
  //   },

  //   sink : (key, msgs) => {
  //     let {scale, date, hour, minsec, band, apid, block, path} = msgs[0];

  //     return airflow.runDag(key, 'full-composite-image', {
  //       scale, date, hour, minsec, band, apid, block, path
  //     });
  //   },
  // },

  [TOPICS.lightning] : {
    enabled : false,
    dependencies : [TOPICS.productWriter],

    where : msg => (msg.data.apid.match(LIGHTNING_PAYLOAD_APIDS)) && (msg.data.file.base === 'payload.bin'),
    sink : (key, msgs) => {
      let task = TOPICS.lightning;
      let data = msgs[0].data;
      let file = pathUtils.resolve(data.file.dir, data.file.base);

      return rabbitMqWorker.exec({
          module : 'generic-payload-parser/parse-lightning.js',
          args : {
            kafkaExternal : true,
            file : file
          }
        },
        {task, msgs}
      );

      // return rabbitMqWorker.exec(`${CASITA_CMD} generic parse-lightning -p -e --file=${file} `, {task, msgs});
    }
  },

  'lighting-grouped-stats' : {
    enabled : false,
    dependencies : [config.kafka.topics.lightning],
    enabled : false,
    expire : 30,
    where : () => false
  }


}

export default dag
