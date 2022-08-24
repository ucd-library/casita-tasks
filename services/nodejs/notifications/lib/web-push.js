import {pg, logger} from '@ucd-lib/casita-worker';
import webPush from 'web-push';

class WebPushNotifications {

  /**
   * @method register
   * @description given a notification type (eg: thermal-anomaly) and a web push subscription
   * payload, add to database
   * 
   * @param {Object} payload 
   * @param {String} type 
   */
  async register(payload, type) {
    if( typeof payload === 'object' ) {
      payload = JSON.stringify(payload);
    }
    await pg.query(`SELECT add_web_push_notification($1, $2)`, [type, payload]);
    return true;
  }

  /**
   * @method unregister
   * @description remove notification subscription.  if not type is provided, all notifcations are removed
   * 
   * @param {String} endpointUrl 
   * @param {String} type 
   */
  async unregister(endpointUrl, type) {
    let resp;
    if( !type ) {
      resp = await pg.query(`SELECT remove_web_push_notifications($1)`, [endpointUrl]);
      return resp;
    }

    resp = pg.query('SELECT * from web_push_notifications_view WHERE payload->>"endpoint" = $1');

    // if there is only one notification, remove notification and subscription
    // otherwise just remove the notification
    if( resp.rows.length === 1 ) {
      resp = await pg.query(`SELECT remove_web_push_notifications($1)`, [endpointUrl]);
    } else {
      resp = await pg.query(`DELETE FROM web_push_notifications WHERE web_push_notifications_id = $1 AND type = $2`, [resp.rows[0].web_push_notifications_id, type]);
    }

    return resp;
  }


  /**
   * @method sendNotifications
   * @description Given a notification type and payload for notification, send to all web push
   * notifications for the notification type.
   * 
   * @param {String} type 
   * @param {Object} payload 
   */
  async sendNotifications(type, payload) {
    let endpoints = await pg.query('SELECT * from web_push_notifications_view WHERE type = $1', [type]);
    logger.info(`Sending web push notification to ${endpoints.rows.length} endpoints`, payload);

    for( let endpoint of endpoints.rows ) {
      await this.sendNotification(endpoint.payload, payload);
    }
  }

  /**
   * @method sendNotifications
   * @description Given a subscription and payload for notification, send  web push
   * notification.  If the web push response is 410 or 404, the subscription will be 
   * automatically removed
   * 
   * @param {Object} subscription 
   * @param {Object} payload 
   */
  async sendNotification(subscription, payload) {
    if( typeof subscription === 'string' ) {
      subscription = JSON.parse(subscription);
    }

    try {
      let success = await webPush.sendNotification(subscription, payload);
    } catch(e) {
      logger.error('Failed to send webpush notification', e, subscription, payload);
      
      // gone or doesn't exist 
      // 410 Gone: the push subscription is expired and no longer exists 
      // (e.g. the user has revoked permission for push notifications from browser settings)
      if( e.statusCode === 410 || e.statusCode === 404 ) {
        logger.info('Removing webpush subscription', subscription.payload);
        await pg.query(`SELECT remove_web_push_notifications($1)`, [subscription.endpoint]);
      }
    }
  }

  /**
   * @method getNotifications
   * @description get all notification types given a subscription endpoint url.  This
   * is useful for verifing a clients notifications are registered on the backend.
   * 
   * @param {String} subscriptionEndpoint url
   * @returns {Promise} resolves to {Array<Object>}
   */
  async getNotifications(subscriptionEndpoint) {
    let resp = await pg.query('SELECT * FROM web_push_notifications_view WHERE payload->>"endpoint" = $1', [subscriptionEndpoint]);
    return resp.rows;
  }

}

const instance = new WebPushNotifications();
export default instance;
