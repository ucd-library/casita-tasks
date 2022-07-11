// make sure topic names match node-commons/config.js => kafka.topics[*].name

// this should match the num of replicates
const DEFAULT_PARTITIONS = 3;

const topics = [
  {
    name : 'goes-decoder',
    partitions : DEFAULT_PARTITIONS,
    options : {
      'retention.ms' : (1000 * 60 * 60 * 4),  // 4 hours
      'max.message.bytes' : 100000000 // 100mb
    }
  },
  {
    name : 'goes-nfs-product',
    partitions : DEFAULT_PARTITIONS,
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
    name : 'block-composite-image',
    partitions : DEFAULT_PARTITIONS
  },
  {
    name : 'block-composite-image-ext',
    partitions : 1
  },
  {
    name : 'ring-buffer',
    partitions : DEFAULT_PARTITIONS
  },
  {
    name : 'ring-buffer-ext',
    partitions : 1
  }
];
export default topics;