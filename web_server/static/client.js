/* eslint semi: ["error", "always", { "omitLastInOneLineBlock": true}] */

const io = require('socket.io-client');

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

var domain = '127.0.0.1';
var port = '80';
var socket = io(`http://${domain}:${port}/socket.io`);

var latestTimestamp = 0;

socket.on('connect', function () {
  socket.emit('connection_confirmed');
  console.log('connected!');
});

socket.on('connection_check', function () {
  socket.emit('connection_confirmed');
  console.log('sending connection confirmation');
});

socket.on('data_full', function (data) {
  console.log(data.timestamp, formatBytes(data.data_full.byteLength));
  latestTimestamp = data.timestamp;
  socket.emit('data_received', { 'client_latest_timestamp': latestTimestamp });
});

socket.on('data_update', function (data) {
  console.log(data.timestamp, formatBytes(data.data_update.byteLength));
  latestTimestamp = data.timestamp;
  socket.emit('data_received', { 'client_latest_timestamp': latestTimestamp });
});
