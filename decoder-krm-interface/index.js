const app = require('express')();
const http = require('http').createServer(app);
const Busboy = require('busboy');
const _fetch = require('node-fetch');
const FormData = require('form-data');
const path = require('path');
const cp = require('child_process');
const {apidUtils} = require('@ucd-lib/goes-r-packet-decoder');
const {logger, StartSubjectModel} = require('@ucd-lib/krm-node-utils');

let model = new StartSubjectModel({groupId: 'decoder-krm-interface'});
let SATELLITE = process.env.SATELLITE || 'west';

function parse(req, res, next) {
  let body = {
    files : {},
    fields : {}
  }

  var busboy = new Busboy({ headers: req.headers });
  busboy.on('file', function(fieldname, file, filename, encoding, mimetype) {
    body.files[fieldname] = {file, filename, encoding, mimetype, data: Buffer.alloc(0)}
    file.on('data', data => {
      body.files[fieldname].data = Buffer.concat([body.files[fieldname].data, data]);
    });
  });
  busboy.on('field', function(fieldname, value, fieldnameTruncated, valueTruncated, encoding, mimetype) {
    body.fields[fieldname] = value;
  });
  busboy.on('finish', () => {
    req.body = body;
    next();
  });

  req.pipe(busboy);
}

app.post('/', parse, async (req, res) => {
  res.send('ack');
  try {
    await handleReq(req, res);
  } catch(e) {
    logger.error('Failed to proxy decoder request', e);
  }
});

async function handleReq (req, res) {
  if( req.body.fields.type === 'image' ) {
    let count = parseInt(req.body.fields.fragmentsCount || 0);
    if( count === 0 ) return;

    let product = apidUtils.get(req.body.fields.apid);
    if( !product.imageScale && !product.label ) return;

    let header = JSON.parse(req.body.fields['fragment_headers_0']);

    let l = header.imagePayload.UPPER_LOWER_LEFT_X_COORDINATE;
    let t = header.imagePayload.UPPER_LOWER_LEFT_Y_COORDINATE;
    let b = t + header.imagePayload.IMAGE_BLOCK_HEIGHT;
    let r = l + header.imagePayload.IMAGE_BLOCK_WIDTH;

    var date = new Date(946728000000 + header.imagePayload.SECONDS_SINCE_EPOCH*1000);
    var [date, time] = date.toISOString().split('T');
    time = time.replace(/\..*/, '');

    let basePath = path.resolve('/', 
      SATELLITE,
      (product.imageScale || product.label || 'unknown').toLowerCase().replace(/[^a-z0-9]+/g, '-'),
      date,
      time.split(':')[0],
      time.split(':').splice(1,2).join('-'),
      product.band,
      req.body.fields.apid,
      'blocks',
      header.imagePayload.UPPER_LOWER_LEFT_X_COORDINATE+'-'+header.imagePayload.UPPER_LOWER_LEFT_Y_COORDINATE
    );

    let data = req.body.fields;
    for( let i = 0; i < count; i++ ) {
      data['fragment_headers_'+i] = JSON.parse(data['fragment_headers_'+i]);
    }
    await send(path.join(basePath, 'fragment-metadata.json'), JSON.stringify(data));

    for( let i = 0; i < count; i++ ) {
      await send(path.join(basePath, 'fragments', i+'', 'image-fragment-metadata.json'), JSON.stringify(data['fragment_headers_'+i]));

      await send(path.join(basePath, 'fragments', i+'', 'image_fragment.jp2'), req.body.files['fragment_data_'+i].data);
    }

  } else {

    let metadata = req.body.fields;
    metadata.spacePacketHeaders = JSON.parse(metadata.spacePacketHeaders);
    metadata.headers = JSON.parse(metadata.headers)

    var date = new Date(946728000000 + metadata.headers.SECONDS_SINCE_EPOCH*1000);
    var [date, time] = date.toISOString().split('T');
    time = time.replace(/\..*/, '');

    let product = apidUtils.get(req.body.fields.apid);

    let basePath = path.resolve('/', 
      SATELLITE,
      (product.imageScale || product.label || 'unknown').toLowerCase().replace(/[^a-z0-9]+/g, '-'),
      date,
      time.split(':')[0],
      time.split(':').splice(1,2).join('-'),
      req.body.fields.apid
    );

    await send(path.join(basePath, 'metadata.json'), JSON.stringify(metadata));

    let file = req.body.files.data || {};
    await send(path.join(basePath, 'payload.bin'), file.data);
  }
}

async function send(file, data) {
  try {
    await model.send(file, data);
  } catch(e) {
    logger.error('Decoder krm interface failed to send subject: '+file, e);
  }
}

(async function() {
  // wait for kafka connection before we start http server
  await model.connect();

  http.listen(3000, async () => {
    logger.info('goes-r '+SATELLITE+' decoder krm proxy listening on port: 3000')
  });
})()
