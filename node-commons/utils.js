import config from './config.js';

class Utils {

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
    if( xy ) {
      var [x, y] = xy.split('-').map(v => parseInt(v));
    }

    let ms = ''
    if( band.length > 2 ) {
      ms = band;
    }

    return {
      satellite, product, scale, date, hour, minsec, band, ms, apid, x, y,
      datetime : new Date(date+'T'+hour+':'+minsec.replace('-', ':')+'.000Z')
    }
  }

}

const instance = new Utils();
export default instance;