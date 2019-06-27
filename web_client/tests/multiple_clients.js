/* eslint semi: ["error", "always", { "omitLastInOneLineBlock": true}] */

var crypto = require('crypto');
const io = require('socket.io-client');

const sleep = (milliseconds) => {
  return new Promise(resolve => setTimeout(resolve, milliseconds));
};

function formatBytes (bytes, decimals) {
  if (bytes === 0) {
    return '0 Bytes';
  }
  var k = 1024;
  var dm = decimals <= 0 ? 0 : decimals || 2;
  var sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
  var i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

class Client {
  constructor () {
    var uniqueId = crypto.randomBytes(64).toString('base64');
    console.log(`generated unique id: ${uniqueId}`);

    var domain = '192.168.99.100';
    var port = '80';
    var socket = io(`http://${domain}:${port}/socket.io?unique_id=${uniqueId}`, {
      autoConnect: false
    });

    var latestTimestamp = 0;

    socket.on('connect', function () {
      socket.emit('set_client_id', { 'client_id': uniqueId });
      if (latestTimestamp === 0) {
        sleep(250).then(() => {
          socket.emit('request_full', { 'client_id': uniqueId });
        });
      }
      console.log('connected!');
    });

    socket.on('data_full', function (data) {
      console.log(data.timestamp, formatBytes(data.data_full.byteLength), uniqueId.slice(0, 5));
      latestTimestamp = data.timestamp;
      socket.emit('data_received', { 'client_id': uniqueId, 'client_latest_timestamp': latestTimestamp });
    });

    socket.on('data_update', function (data) {
      if (data.timestamp_from !== latestTimestamp) {
        console.log(`data.timestamp_from = ${data.timestamp_from}, latestTimestamp = ${latestTimestamp}`);
        // something's gone wrong and the timestamp_from of the update the server sent doesn't match our latestTimestamp
        // so, request the full data so we can get reset
        socket.emit('request_full', { 'client_id': uniqueId });
      }
      console.log(data.timestamp_to, formatBytes(data.data_update.byteLength), uniqueId.slice(0, 5));
      latestTimestamp = data.timestamp_to;
      socket.emit('data_received', { 'client_id': uniqueId, 'client_latest_timestamp': latestTimestamp });
    });

    socket.open();
  }
};

var clients = [];
for (var i = 0; i < 100; i++) {
  var newClient = new Client();
  clients.push(newClient);
};
