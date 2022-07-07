// make sure topic names match node-commons/config.js => kafka.topics[*].name
const topics = [
  {
    name : 'goes-decoder',
    options : {
      'retention.ms' : (1000 * 60 * 60 * 4),  // 4 hours
      'max.message.bytes' : 100000000 // 100mb
    }
  },
  {
    name : 'goes-nfs-product',
    options : {
      'retention.ms' : (1000 * 60 * 60 * 48),  // 48 hours
    }
  },
  {
    name : 'tasks',
    partitions : parseInt(process.env.MAX_WORKERS || 25),
    options : {
      'retention.ms' : (1000 * 60 * 60 * 48),  // 48 hours
    }
  },
  {
    name : 'block-composite-image'
  },
  {
    name : 'ring-buffer'
  }
];
export default topics;