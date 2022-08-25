import { IncomingWebhook } from '@slack/webhook';

async function sendMessage(url, data) {
  let webhook = new IncomingWebhook(url);
  return webhook.send(data);
};

module.exports = sendMessage;
