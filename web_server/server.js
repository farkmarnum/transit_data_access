const express = require('express')
const http = require('http')
const WebSocket = require('ws')

// CONSTANTS
const port = process.env.WEB_SERVER_PORT || 8000


// APP
const app = express()
const server = http.createServer(app)
app.get('/mta', (req, res) => {
  res.send({ express: 'running' })
})


// WEBSOCKETS
const wss = new WebSocket.Server({ server })

wss.on('connection', (ws) => {
  ws.on('message', (message) => {
    console.log('received: %s', message)
    ws.send(`Hello, you sent -> ${message}`)
  })

  ws.send('Hi there, I am a WebSocket server')
})


// SERVER
server.listen(
  port,
  () => console.log(`Server started, listening on port ${server.address().port}`)
)
