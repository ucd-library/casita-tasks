import express from 'express';
import serveIndex from 'serve-index';
import compression from 'compression';
import {logger, config} from '@ucd-lib/casita-worker';
import httpProxy from 'http-proxy';
import http from 'http';
import Cors from 'cors';
import controllers from './controllers/index.js'

const cors = Cors({
  methods: 'GET,HEAD,PUT,PATCH,POST,DELETE,OPTIONS',
  exposedHeaders : ['content-type', 'link', 'content-disposition', 'content-length', 'pragma', 'expires', 'cache-control'],
  allowedHeaders : ['authorization', 'range', 'cookie', 'content-type', 'prefer', 'slug', 'cache-control', 'accept'],
  credentials: true
});

const app = express();
const server = http.createServer(app);

const proxy = httpProxy.createProxyServer();
proxy.on('error', err => logger.warn('api proxy error', err));

const wsServiceMap = {};

app.use(compression());
app.use(cors);

// handle websocket upgrade requests
server.on('upgrade', (req, socket, head) => {
  for( let hostname in wsServiceMap ) {
    if( wsServiceMap[hostname].test(req.url) ) {
      proxy.ws(req, socket, head, {
        target: 'ws://'+hostname+':3000'+req.url,
        ignorePath: true
      });
      return;
    }
  }
});

// register the /_/task-graph api
app.use('/_', controllers);

// register services
if( config?.rest?.proxyServices ) {
  for( let service of config.rest.proxyServices ) {
    logger.info(`Creating api service route '/_${service.route}' to 'http://${service.hostname}'`);
    
    // required to handle websocket upgrade requests
    wsServiceMap[service.hostname] = new RegExp(`^/_${service.route}(/.*|$)`);

    app.use(wsServiceMap[service.hostname], (req, res) => {
      proxy.web(req, res, {
        target: 'http://'+service.hostname+':3000'+req.originalUrl,
        ignorePath: true
      });
    });
  }
}

app.use(
  express.static(config.fs.nfsRoot), 
  serveIndex(config.fs.nfsRoot, {'icons': true})
);

server.listen(3000, async () => {
  logger.info('api listening to port: 3000');
});