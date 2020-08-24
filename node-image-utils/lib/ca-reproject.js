const fs = require('fs-extra');
const path = require('path');
const PNG = require('pngjs').PNG;


class CaReproject {

  constructor(files) {
    this.files = files;
  }

  async loadFiles() {
    this.data = [];
    for( let file of files ) {
      data.push({
        filename : file,
        png : await this._readFile(file),
        metadata : await this._readMetadata(file)
      });
    }
  }

  async _readMetadata(file) {
    return JSON.parse(await fs.readFile(
      path.join(path.parse(file).dir, 'fragments-metadata.json'), 'utf-8'
    ));
  }

  _readFile(file) {
    return new Promise((resolve, reject) => {
      fs.createReadStream(file)
        .pipe(
          new PNG({
            filterType: 4,
          })
        )
        .on('parsed', function () { resolve(this) })
        .on('error', e => { reject(e) });
    });
  }


  run() {
    // https://www.npmjs.com/package/pngjs#example
    // quinn, put code here
    //
    // this.files is array of file objects. Objects have the following properties:
    //  - filename: string
    //  - png: PNGjs object
    //  - metadata: Object.  contains all fragment info.  Normally use the first fragement for reference
    //    First fragement will show t/l corner for image.  Though file path will contain image project, top/left,
    //    band, time... all sort of goodness.
    //
    // path reference: (there will be some path before /:scale/) 
    // https://github.com/ucd-library/casita-krm-deployment/blob/master/graph/index.js#L16
  }

}

module.exports = CaReproject;