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
  getEvents(opts) {
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

    return pg.query('SELECT * FROM thermal_anomaly_event '+where, args);
  }

}

const instance = new EventQuery();
return instance;