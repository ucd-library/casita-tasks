import fetch from 'node-fetch';


const AIRFLOW_HOST = process.env.AIRFLOW_HOST || 'airflow-webserver:8080';
const BASE_API = `http://${AIRFLOW_HOST}/api/v1/dags`;
const AIRFLOW_USERNAME = process.env._AIRFLOW_WWW_USER_USERNAME || 'airflow';
const AIRFLOW_PASSWORD = process.env._AIRFLOW_WWW_USER_PASSWORD || 'airflow';

class Airflow {

  async runDag(key, dagId, conf) {

    let productTime = new Date(conf.date+'T'+conf.hour+':'+conf.minsec.replace('-', ':'));
    console.log('Product Time diff: '+Math.ceil((Date.now() - productTime.getTime())/1000) );

    const body = {
      dag_run_id : key,
      logical_date : new Date().toISOString(),
      conf
    }
  
    console.log(
      'SENDING!!!!!',
      [BASE_API, dagId, 'dagRuns'].join('/'), 
      {
        method : 'POST',
        headers : {'content-type': 'application/json'}, 
        body : JSON.stringify(body)
      }
    );
  
    let t = Date.now();
    let resp = await this._callPostApi(key, [dagId, 'dagRuns'].join('/'), body);
    console.log('POST time: '+(Date.now() - t));

    return resp;

    // return {success: true};
  }

  async _callPostApi(key, path, body) {
    try { 
      let response = await fetch(
        [BASE_API, path].join('/'),
        {
          method : 'POST',
          headers : {
            'content-type': 'application/json',
            'Authorization': `Basic ${Buffer.from(AIRFLOW_USERNAME+':'+AIRFLOW_PASSWORD).toString('base64')}`
          }, 
          body : JSON.stringify(body)
        }
      );

      if( response.status < 200 || response.status > 299 ) {
        return { 
          success: false, 
          message: `failed to send ${key} to dag. status=${response.status} body=`+(await response.text())
        }
      }
    } catch(e) {
      return {
        success : false,
        message : `failed to send ${key} to dag: ${e.message}`
      }
    }

    return {success: true}
  }

}

const airflow = new Airflow();
export default airflow;