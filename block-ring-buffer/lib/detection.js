const pg = require('./pg');

class Detection {

  async getClassifiedPixels(id, classifier=2) {
    let resp = await pg.query(`WITH pixels AS (
      SELECT * FROM ST_PixelOfValue(get_thermal_classified_product(${id}, ${classifier}), 1)
    )
    SELECT * FROM PIXELS;`);

    for( let pixel of resp.rows ) {
      let mode = await this.getPixelMode(id, pixel.x, pixel.y);
      
    }


  }

  async getPixelMode(id, x, y, groupByFactor=10) {
    let resp = await pg.query(`
      WITH rasters as (
        SELECT rast FROM get_rasters_for_stats(${id}) as stats
        LEFT JOIN blocks_ring_buffer ON stats.blocks_ring_buffer_id = blocks_ring_buffer.blocks_ring_buffer_id
      )
      SELECT ST_Value(rast, ${x}, ${y}) as value from rasters;
    `);

    let grouped = {}, v;
    let sum = 0;
    for( let row of resp.rows ) {
      v = Math.floor(row.value / groupByFactor);
      if( !grouped[v] ) grouped[v] = {value: v, count: 0};
      grouped[v].count++;
      sum += row.value;
    }

    grouped = Object.values(grouped);
    grouped.sort((a, b) => a.count - b.count < 0 ? 1 : -1);
    let mode = grouped[0].value * groupByFactor;
    return mode;
  }

  // todo get standard dev
  // make sure x orders over standard dev

}

let test = new Detection();
test.getPixelMode(8599, 100, 100);