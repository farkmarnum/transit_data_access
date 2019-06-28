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
var ready = false;

class Client {
  constructor (id_) {
    var uniqueId = id_;
    console.log(`generated unique id: ${uniqueId}`);

    const socket = new WebSocket(`ws://${domain}:${port}`);

    var upcomingMessageBinaryLength = 0;
    var upcomingMessageTimestamp = 0;

    socket.onopen = () => {
      console.log('connected!');
      socket.send(`
        {
          "type": "set_client_id",
          "client_id": "${uniqueId}"
        }`);
      sleep(10).then(() => {
        socket.emit(`{"type": "request_full", "client_id": "${uniqueId}"}`);
      });
    };

    socket.onmessage = msg => {
      if (ready) {
        if (messageCount === 0) {
          _t = Date.now();
        }
        messageCount++;
        if (messageCount >= numOfClients) {
          console.log((Date.now() - _t) / 1000);
          messageCount = 0;
          _t = Date.now();
        }
        // console.log(uniqueId, 'message received');
        if (msg instanceof String) {
          var parsed = JSON.parse(msg);
          console.log(parsed);
        } else if (msg instanceof Buffer) {
          console.log('binary message received with size: ', formatBytes(msg.byteLength));
        }
      }
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

ready = true;
