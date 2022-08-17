import config from './config.js';

class Utils {

  constructor() {
    this.PATH_ORDER = ['satellite', 'product', 'date', 'band', 'apid', 'blocks', 'x', 'y'];
  }

  /**
   * @method getDataFromPath
   * @description given a file path, parse out metadata we know from this path
   * 
   * @param {String} file 
   * @returns Object
   */
  getDataFromPath(file) {
    let [satellite, product, date, hour, minsec, band, apid, blocks, xy] = file
      .replace(config.fs.nfsRoot, '')
      .replace(/^\//, '')
      .split('/');

    var x = -1,y = -1;
    if( xy && xy.match(/-/) ) {
      var [x, y] = xy.split('-').map(v => parseInt(v));
    }

    let ms = ''
    if( band.length > 2 ) {
      ms = band;
    }

    return {
      satellite, product, date, hour, minsec, band, ms, apid, x, y,
      datetime : new Date(date+'T'+hour+':'+minsec.replace('-', ':')+'.000Z')
    }
  }

  getPathFromData(metadata) {
    let p = [];
    for( let item of this.PATH_ORDER ) {
      
      // see if we have blocks
      if( item === 'blocks' ) {
        if( metadata.x === undefined ) {
          break;
        } else {
          p.push('blocks');
          continue;
        }
      }


      if( !metadata[item] ) throw new Error(item+' not found in metadata, cannot create path');

      // expand date or just push attribute
      if( item ==='date' ) {
        if( typeof metadata.date === 'string' ) {
          metadata.date = new Date(metadata.date);
        }

        let [date, time] = metadata.date.toISOString().split('T');
        time = time.replace(/\..*/, '');

        p.push(date);
        p.push(time.split(':')[0]);
        p.push(time.split(':').splice(1,2).join('-'));
      } else {
        p.push(metadata[item]);
      }
    }

    return p.join('/');
  }

}

const instance = new Utils();
export default instance;