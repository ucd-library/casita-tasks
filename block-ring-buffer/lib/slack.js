const { IncomingWebhook } = require('@slack/webhook');
const {SecretManagerServiceClient} = require('@google-cloud/secret-manager');

let webhook;
const secretClient = new SecretManagerServiceClient();
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
    text: `️‍🔥 New Thermal Event - ${JSON.stringify(data)}`,
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
