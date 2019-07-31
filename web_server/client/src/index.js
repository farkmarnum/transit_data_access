import React from 'react';
import ReactDOM from 'react-dom';
import './normalize.css';
import './skeleton.css';
import './index.css';
import { DataFull, DataUpdate } from './transit_data_access_pb.js'
import { sleep } from './utils.js'

const dateFormat = require('date-format')
const pako = require('pako')
const crypto = require('crypto');

/// WEBSOCKETS
const uniqueId = crypto.randomBytes(64).toString('base64');
const wsHostname = process.env.WEBSOCKET_SERVER_HOSTNAME || '127.0.0.1'
const wsPort = process.env.WEBSOCKET_SERVER_PORT || 8000
const wsURL = `ws://${wsHostname}:${wsPort}/?unique_id=${uniqueId}`
const ws = new WebSocket(wsURL)


/// DATA INFO
const DATA_FULL = 0
const DATA_UPDATE = 1
let upcomingMessageTimestamp = 0
let upcomingMessageBinaryLength = 0
let upcomingMessageType = DATA_FULL


/// REACT
function RouteList(props) {
  if (!props.routes) {
    return "Routes not found"
  }
  
  // console.log(props.lookup)
  // const routeList = props.routes.map((route, i) => {
  //   return (
  //     <li key={i}>
  //       {props.lookup[route[0]]}
  //     </li>
  //   )
  // })

  // return (
  //   <div>
  //     Routes: <br/>
  //     {routeList}
  //   </div>
  // )
  return "routes"
}


function Data(props) {
  const data = props.data
  // console.log(data)
  if (data === null) {
    return "Waiting for data..."
  }

  const system = data.name
  const route_from_hash = data.routehashLookupMap
  const hash_from_route = data.routehashLookupMap
  const station_lookup = data.stationhashLookupMap
  const routes = data.routesMap
  const stations = data.stationMap
  const transfers = data.transfersMap
  const trips = data.tripsMap

  return (
    <RouteList routes={ routes } lookup={ route_from_hash }/>
  )
}

class Main extends React.Component {
  constructor(props) {
    super(props)
    this.state = ({
        realtimeData: null,
        lastSuccessfulTimestamp: 0,
        connected: false,
        dataStatusFlash: false
    })
  }

  flashDataStatus() {
    this.setState({
      dataStatusFlash: true
    })
    sleep(500).then(() => {
      this.setState({
        dataStatusFlash: false
      })
    })
  }

  decodeZippedProto(compressedBlob) {
    // console.log('Unzipping')
    var fileReader = new FileReader();
    fileReader.onload = (event) => {
        const decompressed = pako.inflate(event.target.result)
        let protobufObj
        if (upcomingMessageType === DATA_FULL) {
          // console.log('Deserializing data_full')
          protobufObj = DataFull.deserializeBinary(decompressed)
          this.setState({
            realtimeData: protobufObj
          })
          console.log(protobufObj)
        } else if (upcomingMessageType === DATA_UPDATE) {
          // console.log('Deserializing data_update')
          protobufObj = DataUpdate.deserializeBinary(decompressed).toObject()
          // TODO: LOAD NEW DATA !!!
        }
        this.flashDataStatus()
    }
    fileReader.readAsArrayBuffer(compressedBlob);
  }

  componentWillMount() {
    ws.onopen = () => {
      console.log(`Websocket connection established!`)
      ws.send(`{ "type": "request_full" }`)
      this.setState({
        connected: true
      })
    }
    ws.onclose = (evt) => {
      console.log('Websocket connection closed!')
      this.setState({
        connected: false
      })
    }
    ws.onerror = (evt) => {
      console.log('Error:', evt)
    }

    ws.onmessage = (msg) => {
      let data = msg.data;
      if (typeof data === 'string') {
        // received a string -- this should be either data_full information or data_update information
        let parsed = JSON.parse(data)
        if (parsed.type === 'data_full') {
          upcomingMessageTimestamp = parseInt(parsed.timestamp)
          upcomingMessageBinaryLength = parseInt(parsed.data_size)
          upcomingMessageType = DATA_FULL
        }
        else if (parsed.type === 'data_update') {
          upcomingMessageTimestamp = parseInt(parsed.timestamp_to)
          upcomingMessageBinaryLength = parseInt(parsed.data_size)
          upcomingMessageType = DATA_UPDATE
          if (this.state.lastSuccessfulTimestamp !== parseInt(parsed.timestamp_from)) {
            console.log(`this.state.lastSuccessfulTimestamp = ${this.state.lastSuccessfulTimestamp}, timestamp_from = ${parsed.timestamp_from}`)
          }
        }
      } else if (typeof data === 'object') {
        // received an object -- this should be either data_full or data_update
        if (data.size !== upcomingMessageBinaryLength) {
          console.log('data.size - upcomingMessageBinaryLength is a difference of: ', data.size - upcomingMessageBinaryLength)
          ws.send(`{ "type": "request_full" }`)
        } else {
          this.decodeZippedProto(data)
          this.setState({
            lastSuccessfulTimestamp: upcomingMessageTimestamp
          })
          upcomingMessageTimestamp = 0
          ws.send(`{
            "type": "data_received",
            "last_successful_timestamp": "${this.state.lastSuccessfulTimestamp}"
          }`)
        }
      } else {
        // received neither a string or object!
        console.log('DATA RECEIVED TYPE = ', data)
      }
    }
  }

  render() {
    let dataStatus
    if (this.state.lastSuccessfulTimestamp !== 0) {
      const dateStr = dateFormat.asString(
        'hh:mm:ss',
        new Date(
          this.state.lastSuccessfulTimestamp * 1000))
      dataStatus = `last data update at ${dateStr}`
    } else {
      dataStatus = ''
    }
    // console.log(this.state.realtimeData)
    return (
      <React.Fragment>
        <div id="header-bar"/>
        <div className="container">
          <div className="row header">
            <span ref="dataStatus" className={
              "absolute-left " +
              (this.state.connected ? "connected " : "disconnected ")
            }>
              <h6>{this.state.connected ? "connected" : "disconnected"}</h6>
            </span>
            <span className="title">
              <h3>Transit Data Access</h3>
            </span>
            <span className={
              "absolute-right " +
              (this.state.dataStatusFlash ? "flash-on " : "flash-off ")
            }>
            <h6>{ dataStatus }</h6>
            </span>
          </div>

          <div className="row data">
            <div className="ten columns offset-by-two">
              <Data data={this.state.realtimeData}/>
            </div>
          </div>
        </div>
      </React.Fragment>
    )
  }
}

// ========================================

ReactDOM.render(
  <Main />,
  document.getElementById('root')
);
