import path from 'path';
import {update as updateConfig} from '../../../node-commons/config.js';

const ROOT = '/casita/tasks/nodejs'
const REFERENCES = {};

class ModuleRunner {

  async load(module) {
    if( !REFERENCES[module] ) {
      REFERENCES[module] = await import(path.join(ROOT, module));
    }
    return REFERENCES[module];
  }

  async run(module, args) {
    let fn = await this.load(module);
    if( fn.default ) fn = fn.default;
    updateConfig(args);
    return fn();
  }

}

const instance = new ModuleRunner();
export default instance;