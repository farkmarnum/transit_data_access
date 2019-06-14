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
var socket = io.connect(`http://${domain}:${port}/socket.io`);

var latestTimestamp = 0;

/*
socket.on('new_data', function (data) {
  socket.emit('data_request', { 'client_latest_timestamp': latestTimestamp });
});
*/

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

socket.on('multiple_data_updates', function (data) {
  for (const [timestamp, dataUpdate] of Object.entries(data)) {
    console.log(timestamp, formatBytes(dataUpdate.byteLength));
  }
  var timestamps = Object.keys(data).map(key => parseInt(key));
  latestTimestamp = Math.min(...timestamps);
  socket.emit('data_received', { 'client_latest_timestamp': latestTimestamp });
});
