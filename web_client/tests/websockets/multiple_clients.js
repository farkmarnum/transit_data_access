/* eslint semi: ["error", "always", { "omitLastInOneLineBlock": true}] */

const WebSocket = require('ws');
var crypto = require('crypto');

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

var domain = '192.168.99.100';
var port = '9000';

var messageCount = 0;

var _t;

class Client {
  constructor (id_) {
    var uniqueId = id_;
    console.log(`generated unique id: ${uniqueId}`);

    const socket = new WebSocket(`ws://${domain}:${port}/ws?unique_id=${uniqueId}`);

    var upcomingMessageBinaryLength = 0;
    var upcomingMessageTimestamp = 0;
    var lastSuccessfulTimestamp = 0;

    socket.onopen = () => {
      console.log('connected!');
      socket.send(`
        {
          "type": "set_client_id",
          "client_id": "${uniqueId}"
        }`);
      socket.send(`
        {
          "type": "request_full",
          "client_id": "${uniqueId}"}
        `);
    };

    socket.onmessage = (msg) => {
      var data = msg.data;
      if (typeof data === 'string') {
        // console.log(uniqueId, 'string message received');
        var parsed = JSON.parse(data);
        // console.log(parsed);
        if (parsed.type === 'data_full') {
          process.stdout.write('F');
          // console.log('data_full received');
          upcomingMessageTimestamp = parsed.timestamp;
          upcomingMessageBinaryLength = parsed.data_size;
        } else if (parsed.type === 'data_update') {
          process.stdout.write('U');
          // console.log('data_update received');
          upcomingMessageTimestamp = parsed.timestamp_to;
          upcomingMessageBinaryLength = parsed.data_size;
          if (lastSuccessfulTimestamp !== parsed.timestamp_from) {
            // console.log('hm, something is wrong with timestamp_from');
          }
        }
      } else if (data instanceof Buffer) {
        process.stdout.write('B');
        var dataSize = data.byteLength;
        // console.log(`binary message received with size: ${dataSize} (upcomingMessageBinaryLength is ${upcomingMessageBinaryLength})`);
        if (dataSize !== upcomingMessageBinaryLength) {
          console.log('dataSize - upcomingMessageBinaryLength is a difference of: ', dataSize - upcomingMessageBinaryLength);
        }
        lastSuccessfulTimestamp = upcomingMessageTimestamp;
        socket.send(`
          {
            "type": "data_received",
            "client_id": "${uniqueId}",
            "last_successful_timestamp": "${lastSuccessfulTimestamp}",
          }`);
      };
    };
  }
}

var numOfClients = process.argv.slice(2);
var ids = {};
for (var i = 0; i < numOfClients; i++) {
  ids[i] = crypto.randomBytes(64).toString('base64');
};

var clients = [];
async function createClient (id_) {
  clients.push(new Client(id_));
}

for (i = 0; i < numOfClients; i++) {
  createClient(ids[i]);
};
