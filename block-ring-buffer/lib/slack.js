const { IncomingWebhook } = require('@slack/webhook');
const { CloudBuildClient } = require('@google-cloud/cloudbuild');
const {SecretManagerServiceClient} = require('@google-cloud/secret-manager');
const { Storage } = require('@google-cloud/storage');
const fetch = require('node-fetch');

const url = process.env.SLACK_WEBHOOK_URL;
const webhook = new IncomingWebhook(url);
const secretClient = new SecretManagerServiceClient();
const SECRET_NAME = 'slack-goesr-thermal-event-webook';

// subscribeSlack is the main function called by Cloud Functions.
module.exports = async sendMessage(data) => {
  const msg = createSlackMessage(data);
  const metadata = await getBuildInformation(build);


  // Send message to Slack.
  const message = createSlackMessage(build, metadata);
  await webhook.send(message);

};

async function loadLatestSecret(name) {
  let resp = await secretClient.accessSecretVersion({
    name: `projects/digital-ucdavis-edu/secrets/${name}/versions/latest`
  });
  return resp[0].payload.data.toString('utf-8');
}

// createSlackMessage creates a message from a build object.
const createSlackMessage = (build, metadata) => {

 return {
    text: `${title}Build ${build.id} - ${build.status}
${substitutions}${imagesText}`,
    mrkdwn: true,
    attachments: [
      {
        title: 'Build logs',
        title_link: build.logUrl,
        fields: []
      }
    ]
  };
  return message;
}