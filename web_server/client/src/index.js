import React from 'react'
import ReactDOM from 'react-dom'
import './styles/normalize.css'
import './styles/skeleton.css'
import './styles/index.scss'
import  { devLog,
          sleep,
          processData,
          dataReceivedMsg,
          requestFullMsg,
        } from './utils.js'
import { ArrivalsByRoute } from './components/arrivalsByRoute.js'
import { ArrivalsByStation } from './components/arrivalsByStation.js'

const protobuf = require("protobufjs")
const dateFormat = require('dateformat')
const pako = require('pako')
const crypto = require('crypto')


/// CONSTANTS / ENV
const DATA_FULL = 0
const DATA_UPDATE = 1
let upcomingMessageTimestamp = 0
let upcomingMessageBinaryLength = 0
let upcomingMessageType = DATA_FULL


/// OBJECTS (initialzed later)
let DataFull
let DataUpdate
let ws


/// REACT COMPONENTS ///
class DataInterface extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      selectedOption: null
    }
  }

  setSelectedOption = (option) => {
    this.setState({
      selectedOption: (this.state.selectedOption !== option) ? option: ""
    })
  }

  render() {
    if (this.props.data === null) {
      return (
        <div style={{color: "#fff"}}>
          Waiting for data...
        </div>
      )
    }
    if (!this.props.data.routes) {
      return "Routes not found"
    }

    let arrivalsDisplay
    if (this.state.selectedOption === "route") {
      arrivalsDisplay = (
        <ArrivalsByRoute
          data={this.props.data}
          updatedTrips={this.props.updatedTrips}
        />
      )
    } else if (this.state.selectedOption === "station") {
      arrivalsDisplay = (
        <ArrivalsByStation
          data={this.props.data}
          updatedTrips={this.props.updatedTrips}
        />
      )
    }
    return (
      <React.Fragment>
        <div className="arrival-view-options">
          <button
            className={(this.state.selectedOption === "route") ? "selected" : ""}
            onClick={() => this.setSelectedOption("route")}
          >
            Arrivals by<br/>Route
          </button>
          <button
            className={(this.state.selectedOption === "station") ? "selected" : ""}
            onClick={() => this.setSelectedOption("station")}
          >
            Arrivals by<br/>Station
          </button>
        </div>
        {arrivalsDisplay}
      </React.Fragment>
    )
  }
}


class Main extends React.Component {
  constructor(props) {
    super(props)
    this.state = ({
        unprocessedData: null,
        processedData: null,
        lastSuccessfulTimestamp: 0,
        connected: false,
        dataStatusFlash: false,
        updatedTrips: new Set()
    })
    this.setUpWebSocket = this.setUpWebSocket.bind(this)
    this.updateRealtimeData = this.updateRealtimeData.bind(this)

    // LOAD PROTOBUF
    protobuf.load(`${process.env.PUBLIC_URL}/transit_data_access.proto`).then((root) => {
      DataFull = root.lookupType('transit_data_access.DataFull')
      DataUpdate = root.lookupType('transit_data_access.DataUpdate')

      /// THEN, SET UP WEBSOCKET
      this.setUpWebSocket()
    })
  }

  setUpWebSocket() {
    const uniqueId = crypto.randomBytes(64).toString('base64')
    const wsHost = window.location.hostname
    const wsPort = process.env.WEBSOCKET_SERVER_PORT || 8000
    // const wsPath = process.env.WEBSOCKET_PATH || '/ws'
    const wsPath = ''
    const wsURL = `ws://${wsHost}:${wsPort}${wsPath}/?unique_id=${uniqueId}`
    ws = new WebSocket(wsURL)

    ws.onopen = () => {
      ws.send(requestFullMsg())
      this.setState({
        connected: true
      })
    }
    ws.onclose = (evt) => {
      this.setState({
        connected: false
      }, () => {
        setTimeout(this.setUpWebSocket(), 250)
      })
    }
    ws.onerror = (evt) => {
      console.error(evt)
    }

    ws.onmessage = (msg) => {
      let data = msg.data
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
            devLog(`this.state.lastSuccessfulTimestamp = ${this.state.lastSuccessfulTimestamp}, timestamp_from = ${parsed.timestamp_from}`)
          }
        }
      } else if (typeof data === 'object') {
        // received an object -- this should be either data_full or data_update
        if (data.size !== upcomingMessageBinaryLength) {
          devLog('data.size - upcomingMessageBinaryLength is a difference of: ', data.size - upcomingMessageBinaryLength)
          ws.send(requestFullMsg())
        } else {
          this.decodeZippedProto(data)
          this.setState({
            lastSuccessfulTimestamp: upcomingMessageTimestamp
          }, () => {
            upcomingMessageTimestamp = 0
            ws.send(dataReceivedMsg(this.state.lastSuccessfulTimestamp))
          })
        }
      } else {
        // received neither a string or object!
        devLog('DATA RECEIVED TYPE = ', data)
      }
    }
  }


  updateRealtimeData(update) {
    let updatedTrips = new Set()
    let data = this.state.unprocessedData
    let added, deleted, modified

    // Added arrivals:
    if (update.arrivals) {
      added = update.arrivals.added
      Object.keys(added).forEach(tripHash => { // tripHash := String
        updatedTrips.add(tripHash)
        Object.entries(added[tripHash].arrival).forEach(elem => {
          let stationArrival = elem[1]
          data.trips[tripHash].arrivals[stationArrival.stationHash] = stationArrival.arrivalTime
          if (!stationArrival.stationHash || !stationArrival.arrivalTime) devLog(stationArrival)
        })
      })

      // Deleted arrivals:
      if (update.arrivals.deleted) {
        deleted = update.arrivals.deleted.tripStationDict
        Object.keys(deleted).forEach(tripHash => {
          updatedTrips.add(tripHash)
          deleted[tripHash].stationHash.forEach(stationHash => {
            try {
              delete data.trips[tripHash].arrivals[stationHash]
            } catch (e) {
              console.log(e, tripHash, data.trips)
              throw new Error()
            }
          })
        })
      }

      // Modified arrivals:
      modified = update.arrivals.modified
      Object.keys(modified).forEach(timeDiffStr => {
        let timeDiff = parseInt(timeDiffStr)
        let dict = modified[timeDiffStr].tripStationDict
        Object.keys(dict).forEach(tripHash => {
          updatedTrips.add(tripHash)
          dict[tripHash].stationHash.forEach(stationHash => {
            let oldArrivalTime = data.trips[tripHash].arrivals[stationHash]
            if (!tripHash || !stationHash) console.log('!!', tripHash, stationHash, oldArrivalTime + timeDiff) // TODO: remove after testing
            else data.trips[tripHash].arrivals[stationHash] = oldArrivalTime + timeDiff
          })
        })
      })
    }

    if (update.trips) {
      // Added trips:
      added = update.trips.added
      added.forEach(obj => {
        let tripHash = obj.tripHash
          , trip = obj.info
        data.trips[tripHash] = trip
      })

      // Deleted trips:
      deleted = update.trips.deleted
      deleted.forEach(tripHash => {
        delete data.trips[tripHash]
      })
    }

    if (update.branch) {
      // Trips with modified branches:
      modified = update.branch
      Object.keys(modified).forEach(tripHash => {
        data.trips[tripHash].branch = modified[tripHash]
      })
    }

    if (update.status) {
      // Trips with modified status:
      // TODO: double check this
      modified = update.status
      Object.keys(modified).forEach(tripHash => {
        data.trips[tripHash].status = modified[tripHash]
      })
    }

    /// Update the data and flash the updated trips:
    const processedData = processData(data)
    this.setState({
      unprocessedData: data,
      processedData: processedData,
      updatedTrips: updatedTrips
    }, (() => {
      this.flashDataStatus()
      setTimeout(
        () => {
          this.setState({
            updatedTrips: new Set()
          })},
        500)
      })
    )
  }


  componentDidMount() {
    // force the React DOM to reload every second to update times specified in seconds:
    this.forceUpdateInterval = setInterval(() => this.setState({
      time: Date.now()
    }), 1 * 1000)
  }
  componentWillUnmount() {
    clearInterval(this.forceUpdateInterval)
  }


  flashDataStatus() {
    this.setState({
      dataStatusFlash: true
    })
    sleep(500).then(() => { // TODO: make this a constant that is 2x the CSS transformation time value
      this.setState({
        dataStatusFlash: false
      })
    })
  }

  loadFull(raw) {
    const unprocessedData = DataFull.decode(raw)
    const processedData = processData(unprocessedData)
    devLog(processedData)
    this.setState({
      unprocessedData: unprocessedData,
      processedData: processedData
    })
    this.flashDataStatus()
  }
  loadUpdate(raw) {
    const update = DataUpdate.decode(raw)
    devLog(update)
    this.updateRealtimeData(update)
  }


  decodeZippedProto(compressedBlob) {
    var fileReader = new FileReader()
    fileReader.onload = (event) => {
        const decompressed = pako.inflate(event.target.result)
        if (upcomingMessageType === DATA_FULL) this.loadFull(decompressed)
        else if (upcomingMessageType === DATA_UPDATE) this.loadUpdate(decompressed)
        else console.error("upcomingMessageType not valid")
    }
    fileReader.readAsArrayBuffer(compressedBlob)
  }


  render() {
    let dataStatus
    if (this.state.lastSuccessfulTimestamp !== 0) {
      const dateStr = dateFormat(
        new Date(this.state.lastSuccessfulTimestamp * 1000),
        "h:MM:sstt")
      dataStatus = `data last updated at ${dateStr}`
    } else {
      dataStatus = ''
    }
    let connected = ws && ws.readyState === 1

    return (
      <React.Fragment>
        <div id="header-bar"/>
        <div id="background"/>
        <div className="container">
          <div className="row header">
            <span ref="dataStatus" className={
              "absolute-left " +
              (connected ? "connected " : "disconnected ")
            }>
              <h6>{connected ? "connected" : "disconnected"}</h6>
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
            <div className="twelve columns">
              <DataInterface data={ this.state.processedData } updatedTrips={ this.state.updatedTrips }/>
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
)
