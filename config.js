//@ts-ignore
// import libConfig from './lib-config.cjs';
// import config from './config.defaults.js';

// export default libConfig(config);

const libConfig = require("./lib-config.cjs");
const config = require("./config.defaults");

module.exports = libConfig(config);
