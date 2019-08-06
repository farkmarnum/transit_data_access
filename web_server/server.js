const http = require('http')
const querystring = require('querystring')
const WebSocket = require('ws')
const Redis = require('ioredis')


/// /// CONSTANTS /// ///
const hostname = '0.0.0.0'
const port = process.env.WEBSOCKET_SERVER_PORT || 8000
const redisHostname = process.env.REDIS_HOSTNAME || 'redis_server'
const redisPort = process.env.REDIS_PORT || 6379


/// /// DATA /// ///
let dataFull = null
let dataUpdates = []
let latestTimestamp = 0

let clients = {}
class Client {
  constructor (ws) {
    this.ws = ws
    this.conected = true
    this.lastSuccessfulTimestamp = 0
  }
}


/// /// REDIS /// ///
Redis.Command.setReplyTransformer('hgetall', function (result) {
  if (Array.isArray(result)) {
    var obj = {}
    for (var i = 0; i < result.length; i += 2) {
      obj[result[i]] = result[i + 1]
    }
    return obj
  }
  return result
})

// Redis client for get()
const redis = new Redis({ host: redisHostname, port: redisPort })
redis.on('connect', () => { console.log('Redis client connected') })
redis.on('error', (err) => { console.log('Redis error: ' + err) })

// get the data:
function getRedisData () {
  redis
    .multi()
    .get('realtime:current_timestamp')
    .getBuffer('realtime:data_full')
    .hgetallBuffer('realtime:data_diffs')
    .exec((requestErr, results) => {
      if (requestErr) {
        console.log(requestErr)
      } else {
        // results === [[err, result], [err, result], [err, result]]
        let errors = [results[0][0], results[1][0], results[2][0]]
        if (errors.some(err => err)) {
          console.log(errors.filter(err => err))
        } else {
          latestTimestamp = results[0][1]
          dataFull = results[1][1]
          dataUpdates = results[2][1]
          pushToAll()
        }
      }
    })
}
getRedisData()


// redisPubSub client for subscribe()
const redisPubSub = new Redis({ host: redisHostname, port: redisPort })
redisPubSub.on('connect', () => { console.log('redisPubSub client connected') })
redisPubSub.on('error', (err) => { console.log('redisPubSub error: ' + err) })
redisPubSub.on('message', (channel, msg) => {
  if (channel === 'realtime_updates' && msg === 'new_data') {
    getRedisData()
  }
})
redisPubSub.subscribe('realtime_updates')


/// /// SERVER /// ///
const server = http.createServer((req, res) => {
  res.write('This API uses websockets only.') // write a response to the client
  res.end() // end the response
})

server.listen(port, hostname, 1024, (err) => {
  if (err) {
    console.log(`Server error: ${err}`)
  } else {
    console.log(`Server started, listening @ ${server.address().address}:${server.address().port}`)
  }
})


/// /// WEBSOCKETS /// ///
const wsServer = new WebSocket.Server({ server })

wsServer.on('connection', (ws, request) => {
  const clientId = querystring.parse(request.url.slice(2)).unique_id
  console.log(`new client w/ ID ${clientId}`)
  clients[clientId] = new Client(ws, true, 0)

  ws.on('message', (msg) => {
    // console.log(msg)
    try {
      const parsed = JSON.parse(msg)
      if (parsed.type === 'data_received') {
        clients[clientId].lastSuccessfulTimestamp = parsed.last_successful_timestamp
      } else if (parsed.type === 'request_full') {
        if (dataFull !== null) {
          sendFull(clients[clientId])
        } else {
          clients[clientId].ws.send(`
{
  "type": "error",
  "error": "no_data"
}`)
        }
      }
    } catch (err) {
      console.log(err)
    }
  })
})

function sendFull (client) {
  client.ws.send(`{
    "type": "data_full",
    "timestamp": "${latestTimestamp}",
    "data_size": "${dataFull.byteLength}"
  }`)
  client.ws.send(dataFull)
}
function sendUpdate (client) {
  const update = dataUpdates[client.lastSuccessfulTimestamp]
  client.ws.send(`{
    "type": "data_update",
    "timestamp_from": "${client.lastSuccessfulTimestamp}",
    "timestamp_to": "${latestTimestamp}",
    "data_size": "${update.byteLength}"
  }`)
  client.ws.send(update)
}
function pushToAll () {
  console.log(`Received new data w/ timestamp ${latestTimestamp}, pushing it to clients`)
  for (const [clientID, client] of Object.entries(clients)) {
    if (client.ws.readyState === WebSocket.OPEN) {
      if (client.lastSuccessfulTimestamp in dataUpdates) {
        sendUpdate(client)
      } else {
        sendFull(client)
      }
    }
  }
}
