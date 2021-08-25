const { IncomingWebhook } = require('@slack/webhook');
const {SecretManagerServiceClient} = require('@google-cloud/secret-manager');
const fs = require('fs');

let gConfig = {};
if( fs.existsSync('/etc/google/service-account.json') ) {
  gConfig.keyFilename = '/etc/google/service-account.json';
}

let webhook;
const secretClient = new SecretManagerServiceClient(gConfig);
const SECRET_NAME = 'slack-goesr-thermal-event-webook';

// subscribeSlack is the main function called by Cloud Functions.
async function sendMessage(data) {
  await webhook.send(createSlackMessage(data));
};

async function loadLatestSecret(name) {
  let resp = await secretClient.accessSecretVersion({
    name: `projects/digital-ucdavis-edu/secrets/${name}/versions/latest`
  });
  return resp[0].payload.data.toString('utf-8');
}

// createSlackMessage creates a message from a build object.
const createSlackMessage = (data) => {
 return {
    text: `️‍🔥 New Thermal Event - ${JSON.stringify(data)} - 
    https://data.casita.library.ucdavis.edu/_/thermal-anomaly/kml/network?thermal_event_id=${data.thermal_event_id}`,
    mrkdwn: true,
    attachments: []
  };
}

(async function() {
  let url = process.env.SLACK_WEBHOOK_URL;
  if( !url ) url = await loadLatestSecret(SECRET_NAME);
  webhook = new IncomingWebhook(url);
})();

module.exports = sendMessage;
