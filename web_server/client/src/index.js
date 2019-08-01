import React from 'react';
import ReactDOM from 'react-dom';
import './normalize.css';
import './skeleton.css';
import './index.css';
import { sleep } from './utils.js'

const protobuf = require("protobufjs")
const dateFormat = require('dateformat')
const pako = require('pako')
const crypto = require('crypto');

const uniqueId = crypto.randomBytes(64).toString('base64');

/// DATA INFO
const DATA_FULL = 0
const DATA_UPDATE = 1
let upcomingMessageTimestamp = 0
let upcomingMessageBinaryLength = 0
let upcomingMessageType = DATA_FULL

// OBJECTS (initialzed later)
let DataFull
let DataUpdate
let ws


/// REACT
/// /// /// TODO: ORGANIZE THIS WITH A BETTER CLASS HEIRARCHY SO IT'S READABLE! ALSO COMMENTS!!! AND DOCSTRINGS!!!!!!
function RouteArrivals(props) {
  if (props.selectedRoute && props.selectedFinalStation) {
    return (
      <div className='stations'>
        <code>*Known Issue: some stations are out of order</code>
        {
          props.selectedRoute.stations.map((stationHash, i) => {
            const now = Date.now()
            const station = props.data.stations[stationHash]
            let arrivalsForRoute = Object.keys(station.arrivals).filter(arrivalTime => {
              let tripId = station.arrivals[arrivalTime][props.selectedFinalStation]
              if(!tripId) return false
                let routeHash = props.data.trips[tripId].branch.routeHash
                  return (
                  routeHash === props.selectedRouteHash &&
                  now - arrivalTime > -30
                )
              // console.log(station.arrivals[arrivalTime], props.selectedFinalStation)
            })
            arrivalsForRoute = arrivalsForRoute.sort().slice(0, 3)

            // convert each timeDiff (# of secs) to a text representation (30s, 4m, 12:34p, etc), and a styling based on how soon it is
            let arrivalTimeDiffsWithFormatting = arrivalsForRoute.map(arrivalTimeStr => { // TODO: why is this a string
              let arrivalTime = parseInt(arrivalTimeStr)
              let timeDiff = Math.floor(arrivalTime - (now / 1000))
              if (timeDiff < 15) {
                return ["now", "very-soon"]
              } else if (timeDiff < 60) {
                return [Math.floor(timeDiff) + "s", "very-soon"]
              } else if (timeDiff < 10 * 60) {
                return [Math.floor(timeDiff / 60) + "m", "soon"]
              } else {
                return [dateFormat(new Date(arrivalTime * 1000), 'h:MMt'), ""]
              }
            })
            /// TODO: remove dupplicate "now" entries...
            // arrivalTimeDiffs = [...new Set(arrivalTimeDiffs)] // TODO: this is inefficient...

            if (arrivalTimeDiffsWithFormatting.length === 0) return null
            return (
              <div className='station' key={i}>
                <span className='station-name'>
                  {station.name}
                </span>
                {
                  arrivalTimeDiffsWithFormatting.map((timeDiffWithFormatting, i) => {
                    let[timeDiff, formattingClass] = timeDiffWithFormatting
                    return (
                      <span className={"arrival-time " + formattingClass} key={i}>
                        {timeDiff}
                      </span>
                    )
                  })
                }
              </div>
            )
          })
        }
      </div>
    )
  }
  return null
}

class RouteList extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      selectedRouteHash: null,
      selectedFinalStation: null,
      finalStations: null
    }
    this.routes = this.props.data.routes
    this.routeNameLookup = this.props.data.routeNameLookup
    this.stations = this.props.data.stations
  }

  routeClicked(routeHash) {
    const newRouteHash = (routeHash === this.state.selectedRouteHash) ? null : routeHash
    const finalStations = (newRouteHash === null) ? null : this.routes[newRouteHash].finalStations
    this.setState({
      selectedRouteHash: newRouteHash,
      finalStations: finalStations,
      selectedFinalStation: null
    })
  }

  finalStationClicked(stationHash) {
    this.setState({
      selectedFinalStation: (stationHash === this.state.selectedFinalStation) ? null : stationHash
    })

  }

  render() {
    if (!this.props.data.routes) {
      return "Routes not found"
    }

    const routeInfos = Object.entries(this.routes).map((elem, i) => {
      let routeHash = parseInt(elem[0])
      let routeInfo = elem[1]
      let routeName = this.routeNameLookup[routeHash]
      // console.log([routeName, routeInfo, i])
      return [routeName, routeInfo, routeHash, i]
    })

    const selectedRoute = this.routes[this.state.selectedRouteHash]
    const selectedRouteName = this.routeNameLookup[this.state.selectedRouteHash]

    let finalStationsRender
    if (this.state.finalStations !== null && this.state.finalStations.size > 0) {
      finalStationsRender = [...this.state.finalStations].map((stationHash, i) => {
        return (
          <button
            className={"final-station " + ((this.state.selectedFinalStation === stationHash) ? "selected" : "")}
            onClick={this.finalStationClicked.bind(this, stationHash)}
            key={i}
          >
            {
              "to " + this.stations[stationHash].name
            }
          </button>
        )
      })
    } else {
      if (this.state.selectedRouteHash !== null) {
        finalStationsRender = "No trains currently running on this route."
      } else {
        finalStationsRender = this.state.selectedRouteHash
      }
    }
    return (
      <div>
        <h5> Arrivals by Route TESTING</h5>
        <div className="route-name-list">
          {
            routeInfos.sort().map(elem => {
              const routeName = elem[0]
                  , routeInfo = elem[1]
                  , routeHash = elem[2]
                  , i         = elem[3]

              const routeColor = "#" + ("00"+(Number(routeInfo.color).toString(16))).slice(-6)
              let style
              if (routeHash === this.state.selectedRouteHash) {
                style = {backgroundColor: routeColor, color: 'white'}
              } else {
                style = {color: routeColor, backgroundColor: 'white'}
              }
              return (
                <button
                  key={i}
                  className={"route-name"}
                  style={style}
                  title={routeInfo.desc}
                  onClick={this.routeClicked.bind(this, routeHash)}
                >
                  { routeName }
                </button>
              )
            })
          }
        </div>
        <div className="final-stations">
          { finalStationsRender }
        </div>
        <RouteArrivals
          className="route-arrivals"
          selectedRoute={selectedRoute}
          selectedRouteHash={this.state.selectedRouteHash}
          selectedRouteName={selectedRouteName}
          selectedFinalStation={this.state.selectedFinalStation}
          data={this.props.data}
        />
      </div>
    )
  }
}


function Data(props) {
  const data = props.data
  if (data === null) {
    return "Waiting for data..."
  }

  // add a val:key version of the lookup for routes (hash -> name)
  data.routeNameLookup = {}
  for (let key in data.routehashLookup) {
    let val = data.routehashLookup[key]
    data.routeNameLookup[val] = key
  }

  // add a val:key version of the lookup for routes (hash -> name)
  // TODO: unneeded?
  data.stationNameLookup = {}
  for (let key in data.stationhashLookup) {
    let val = data.stationhashLookup[key]
    data.stationNameLookup[val] = key
  }

  // populate the arrivals list for each station in props.stations
  // also, create a set of finalStations for each route
  for (let stationHash in data.stations) {
    data.stations[stationHash].arrivals = {}
  }
  for (let routeHash in data.routes) {
    data.routes[routeHash].finalStations = new Set()
  }
  // console.log(Object.keys(data.routes))
  for (let tripHash in data.trips) {
    // console.log(tripHash)
    const trip = data.trips[tripHash]
    for (let elem of Object.entries(trip.arrivals)) {
      const stationHash = elem[0]
          , arrivalTime = elem[1]
      if (!data.stations[stationHash].arrivals[arrivalTime]) {
        data.stations[stationHash].arrivals[arrivalTime] = {}
      }
      try {
        data.stations[stationHash].arrivals[arrivalTime][trip.branch.finalStation] = tripHash
        data.routes[trip.branch.routeHash.toString()].finalStations.add(trip.branch.finalStation) // TODO figure out why each routeHash in data.routes is a string...
      } catch (err) {
        // console.log(trip.branch.routeName)
      }
    }
  }
  return <RouteList data={ data } />
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
    this.setUpWebSocket = this.setUpWebSocket.bind(this)

    // LOAD PROTOBUF
    protobuf.load(`${process.env.PUBLIC_URL}/transit_data_access.proto`).then((root) => {
      DataFull = root.lookupType('transit_data_access.DataFull')
      DataUpdate = root.lookupType('transit_data_access.DataUpdate')

      /// THEN, SET UP WEBSOCKET
      this.setUpWebSocket()
    })
  }


  setUpWebSocket() {
    const hostname = window.location.hostname
    const wsPort = process.env.WEBSOCKET_SERVER_PORT || 8000
    const wsURL = `ws://${hostname}:${wsPort}/?unique_id=${uniqueId}`
    ws = new WebSocket(wsURL)

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
      }, () => {
        setTimeout(this.setUpWebSocket(), 250)
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

  updateRealtimeData(protobufObj) {

  }

  componentDidMount() {
    this.forceUpdateInterval = setInterval(() => this.setState({ time: Date.now() }), 60 * 1000);
  }
  componentWillUnmount() {
    clearInterval(this.forceUpdateInterval);
  }

  decodeZippedProto(compressedBlob) {
    // console.log('Unzipping')
    var fileReader = new FileReader();
    fileReader.onload = (event) => {
        const decompressed = pako.inflate(event.target.result)
        let protobufObj

        if (upcomingMessageType === DATA_FULL) {
          protobufObj = DataFull.decode(decompressed)
          this.setState({
            realtimeData: protobufObj
          })
          console.log(protobufObj)

        } else if (upcomingMessageType === DATA_UPDATE) {
          protobufObj = DataUpdate.decode(decompressed)
          console.log(protobufObj)
        }
        this.flashDataStatus()
    }
    fileReader.readAsArrayBuffer(compressedBlob);
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
            <div className="ten columns offset-by-one">
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
