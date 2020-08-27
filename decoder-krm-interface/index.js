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

    let basePath = path.resolve('/', 
      (product.imageScale || product.label || 'unknown').toLowerCase().replace(/[^a-z0-9]+/g, '-'),
      date,
      time.split('.')[0].replace(/:/g, '-'),
      product.band,
      req.body.fields.apid,
      'cells',
      header.imagePayload.UPPER_LOWER_LEFT_X_COORDINATE+'-'+header.imagePayload.UPPER_LOWER_LEFT_Y_COORDINATE
    );

    let data = req.body.fields;
    await send(path.join(basePath, 'fragment-metadata.json'), JSON.stringify(data));

    for( let i = 0; i < count; i++ ) {
      await send(path.join(basePath, 'fragments', i+'', 'image-fragment-metadata.json'), req.body.fields['fragment_headers_'+i]);

      await send(path.join(basePath, 'fragments', i+'', 'image_fragment.jp2'), req.body.files['fragment_data_'+i].data);
    }

    // let payload = {
    //   top: t, left: l, bottom: b, right: r, 
    //   time: header.imagePayload.SECONDS_SINCE_EPOCH,
    //   apid: req.body.fields.apid
    // };

    // if( config.goesProducts[req.body.fields.apid] ) {
    //   sendToWorker(Object.assign({payload}, req.body));
    // } else {
    //   process.send({event: 'boundary', payload});
    // }


  // } else if( req.body.fields.apid === '302' ) {
    // let data = lightningPayloadParser.parseFlashData(req.body.files.data.data);

    // let tmp = [];
    // for( let i = 0; i < data.length; i++ ) {
    //   let latlng = await project(data[i].flash_lon, data[i].flash_lat);
    //   tmp.push({lon: latlng[0], lat: latlng[1]});
    // }

    // process.send({event: 'lightning-events', payload: {data: tmp, apid: req.body.fields.apid}});
  // } else if( req.body.fields.apid === '301' ) {
    // let data = lightningPayloadParser.parseEventData(req.body.files.data.data);
    // process.send({event: 'lightning-strike-count', payload: {data: data.length}});
  } else {

    let metadata = req.body.fields;
    metadata.spacePacketHeaders = JSON.parse(metadata.spacePacketHeaders);
    metadata.headers = JSON.parse(metadata.headers)

    var date = new Date(946728000000 + metadata.headers.SECONDS_SINCE_EPOCH*1000);
    var [date, time] = date.toISOString().split('T');

    let product = apidUtils.get(req.body.fields.apid);

    let basePath = path.resolve('/', 
      (product.imageScale || product.label || 'unknown').toLowerCase().replace(/[^a-z0-9]+/g, '-'),
      date,
      time.split('.')[0].replace(/:/g, '-'),
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

http.listen(3000, async () => {
  await model.connect();
  logger.info('goes-r decoder krm proxy listening on port: 3000')
});