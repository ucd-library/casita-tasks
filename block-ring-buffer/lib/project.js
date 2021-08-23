const Req = 6378137; // semi_major_axis
const Rpol = 6356752.31414; // semi_minor_axis
const H = 42164160; // perspective_point_height + semi_major_axis

// const LAMDA0 = -1.308996939; // Satellite East longitude of projection origin
const LAMDA0 = -2.39110107523; // Satellite West longitude of projection origin

const RAD_OFFSET = 0.151872;
const RAD_4KM_GRID = 0.000056;

function S(x, y) {
  let vRs = Rs(x, y);
  return {
    x : vRs * Math.cos(x) * Math.cos(y),
    y : -1 * vRs * Math.sin(x),
    z : vRs * Math.cos(x) * Math.sin(y)
  };
}

function Rs(x, y) {
  let v = {
    a : a(x, y),
    b : b(x, y),
    c : c()
  };

  return (-1 * v.b - Math.sqrt( Math.pow(v.b, 2) - 4 * v.a * v.c) ) / 2*v.a;
}

function a(x, y) {
  return Math.pow(Math.sin(x), 2) + 
  Math.pow(Math.cos(x), 2) *
  (
    Math.pow(Math.cos(y), 2) +
    (Math.pow(Req, 2) /  Math.pow(Rpol, 2)) *
    Math.pow(Math.sin(y), 2)
  )
}

function b(x, y) {
  return -2 * H * Math.cos(x) * Math.cos(y);
}

function c() {
  return Math.pow(H, 2) - Math.pow(Req, 2);
}

function latitude(vS) {
  return Math.atan(
    (Math.pow(Req,2) / Math.pow(Rpol, 2)) *
    (
      vS.z / (
        Math.sqrt(
          Math.pow(H - vS.x, 2) + Math.pow(vS.y, 2)
        )
      )
    )
  )
}

function longitude(vS) {
  return LAMDA0 - Math.atan(
    vS.y / (H - vS.x)
  )
}

function project(x, y)  {
  let xRad = -1 * RAD_OFFSET + (x * RAD_4KM_GRID);
  let yRad = RAD_OFFSET - (y * RAD_4KM_GRID);

  let vS = S(xRad, yRad);

  let latRad = latitude(vS);
  let lngRad = longitude(vS);

  return {
    latitude : latRad * 180/Math.PI,
    longitude :lngRad * 180/Math.PI
  }
}

module.exports = project;