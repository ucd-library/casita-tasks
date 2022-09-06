import {pg} from '@ucd-lib/casita-worker';

class EventQuery {

  /**
   * @method getEvents
   * @description query for thermal anomaly events
   * 
   * @param {Object} opts 
   * @param {Boolean} opts.active
   * @param {Date} opts.minDate
   * @param {Date} opts.maxDate
   * @param {String} opts.product
   * 
   * @returns {Promise} resolves to Array 
   */
  async getEvents(opts={}) {
    let where = [];
    let args = [];

    if( typeof opts.active === 'boolean' ) {
      where.push('active = $'+(where.length+1));
      args.push(opts.active);
    }

    if( opts.minDate ) {
      where.push('date >= $'+(where.length+1));
      args.push(opts.minDate);
    }

    if( opts.maxDate ) {
      where.push('date <= $'+(where.length+1));
      args.push(opts.maxDate);
    }

    if( opts.product ) {
      where.push('product = $'+(where.length+1));
      args.push(opts.product);
    }

    if( where.length ) where = 'where '+where.join(' AND ');
    else where = '';

    return (await pg.query('SELECT * FROM thermal_anomaly_event '+where, args)).rows;
  }

  async getEvent(id) {
    let event = await pg.query(`SELECT * FROM thermal_anomaly_event WHERE thermal_anomaly_event_id = $1`, [id]);
    if( !event.rows.length ) throw new Error('Unknown thermal_anomaly_event_id '+id);
    event = event.rows[0];

    let times = await pg.query(`SELECT 
        count(*) as pixelcount, 
        max(value) as maxvalue, 
        date 
      FROM 
        thermal_anomaly_event_px 
      WHERE 
        thermal_anomaly_event_id = $1 
      GROUP BY 
        date
      ORDER BY 
        date`, 
      [id]
    );

    event.timestamps = [['timestamp', 'pixelCount', 'maxValue']];
    times.rows.forEach(item => {
      event.timestamps.push([item.date.toISOString(), item.pixelcount, item.maxvalue]);
    });

    return event;
  }

}

const instance = new EventQuery();
export default instance;