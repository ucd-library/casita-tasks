const {config} = require('@ucd-lib/krm-node-utils');

config.decoder = {
  groupId : 'decoder-krm-interface',
  kafka : {
    host : env.DECODER_KAFKA_HOST || 'kafka',
    port : env.DECODER_KAFKA_PORT || 9092,
    topic : env.DECODER_KAFKA_TOPIC || 'goes-r-stream'
  }
};

module.exports = config;