const project = require('./project');
const HOUR = 1000 * 60 * 60;

const COLOR_RAMP = {
  2000 : '0000ff',
  3000 : '005dff',
  4000 : '00a6ff',
  5000 : '00d7fe',
  6000 : '00fffe',
  7000 : '87ffff',
  8000 : 'ffffff'
}

module.exports = function(points, opts) {
  let pixels = {};
  points.forEach(pt => {
    if( !pixels[pt.world_x+'-'+pt.world_y] ) {
      pixels[pt.world_x+'-'+pt.world_y] = []
    }
    pixels[pt.world_x+'-'+pt.world_y].push(pt);
  });

  let arr;
  for( let key in pixels ) {
    arr = pixels[key];
    arr.sort((a, b) => a.date.getTime() < b.date.getTime() ? -1: 1);
    
    let length = arr.length;
    let p2;
    arr.forEach((pt, index) => {
      if( index < length - 2 ) {
        p2 = arr[index+1];

        if( p2.date.getTime() - pt.date.getTime() < HOUR ) {
          pt.end = new Date(p2.date.getTime());
        } else {
          pt.end = new Date(pt.date.getTime()+HOUR);
        }

      } else {
        pt.end = new Date(pt.date.getTime()+HOUR);
      }
    })

  }

  return header() +
    points.map(pt => point(pt)).join('\n') +
  footer();
}

function header(title='GOES-R Thermal Detection') {
  return `<?xml version="1.0" encoding="UTF-8"?>
  <kml xmlns="http://www.opengis.net/kml/2.2">
    <Document>
      <name>${title}</name>`
}

function point(point) {
  let tl = project(point.world_x, point.world_y);
  let tr = project(point.world_x+1, point.world_y);
  let br = project(point.world_x+1, point.world_y+1);
  let bl = project(point.world_x, point.world_y+1);

  let ele = Math.floor(point.value);

  let color;
  for( let key in COLOR_RAMP ) {
    if( point.value < key ) {
      color = COLOR_RAMP[key];
      break;
    }
  }
  if( !color ) color = COLOR_RAMP[5000];

  
  return `<Placemark>
  <name>${point.world_x}, ${point.world_y}</name>
  <TimeSpan>
    <begin>${point.date.toISOString()}</begin>
    <end>${point.end.toISOString()}</end>
  </TimeSpan>

  <ExtendedData>
    <Data name="GOES-R Grid">
      <value>${point.world_x}, ${point.world_y}</value>
    </Data>
    <Data name="Lat/Lng">
      <value>${tl.longitude}, ${tl.latitude}</value>
    </Data>
    <Data name="Satellite">
      <value>${point.satellite}</value>
    </Data>
    <Data name="Band">
      <value>${point.apid}, Band ${point.band}</value>
    </Data>
    <Data name="Block">
      <value>${point.product} ${point.block_x}, ${point.block_y}</value>
    </Data>
    <Data name="Datetime">
      <value>${point.date.toISOString()}</value>
    </Data>
    <Data name="Block X,Y">
      <value>${point.pixel_x}, ${point.pixel_y}</value>
    </Data>
    <Data name="Band ${point.band} Value">
      <value>${point.value}</value>
    </Data>
    <Data name="Thermal Event ID">
      <value>${point.thermal_event_id}</value>
    </Data>
    <Data name="Thermal Event Pixel ID">
      <value>${point.thermal_event_px_id}</value>
    </Data>
    <Data name="Thermal Event Pixel Chart">
      <value>https://casita-thermal-px-chart-akwemh35fa-uc.a.run.app/thermal-event-px/${point.thermal_event_px_id}</value>
    </Data>
  </ExtendedData>
  <Style>
    <PolyStyle>
      <color>7f${color}</color>
    </PolyStyle>
  </Style>
  <Polygon>
    <extrude>1</extrude>
    <altitudeMode>relativeToGround</altitudeMode>
    <outerBoundaryIs>
      <LinearRing>
        <coordinates>
          ${tl.longitude},${tl.latitude},${ele} 
          ${tr.longitude},${tr.latitude},${ele}
          ${br.longitude},${br.latitude},${ele}
          ${bl.longitude},${bl.latitude},${ele}
          ${tl.longitude},${tl.latitude},${ele}
        </coordinates>
      </LinearRing>
    </outerBoundaryIs>
  </Polygon>
</Placemark>`
}

function footer() {
  return `</Document>
  </kml>`;
}