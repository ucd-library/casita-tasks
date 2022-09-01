import {slack, config} from '@ucd-lib/casita-worker';
import {SecretManagerServiceClient} from '@google-cloud/secret-manager';

const secretClient = new SecretManagerServiceClient();
let url = '';

async function loadLatestSecret(name) {
  let resp = await secretClient.accessSecretVersion({
    name: `projects/digital-ucdavis-edu/secrets/${name}/versions/latest`
  });
  return resp[0].payload.data.toString('utf-8');
}

async function send(eventId) {
  if( !url ) {
    url = await loadLatestSecret(config.thermalAnomaly.slack.urlSecret);
  }
  
  return slack(url, {
    text: `️‍🔥 New Thermal Anomaly Event - ${eventId} - 
    https://thermal-anomaly.casita.library.ucdavis.edu/event-detail/${eventId}`,
    mrkdwn: true,
    attachments: []
  })
}

export default send;