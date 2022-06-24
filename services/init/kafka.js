const topics = [
  {
    name : 'goes-decoder',
    options : {
      'retention.ms' : (1000 * 60 * 60),
      'max.message.bytes' : 100000000
    }
  },
  {
    name : 'goes-nfs-product'
  }
];
export default topics;