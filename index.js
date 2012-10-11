
module.exports = process.env.CONNECT_MONGO_PLUS_COV
  ? require('./lib-cov/connect-mongo-plus')
  : require('./lib/connect-mongo-plus');
