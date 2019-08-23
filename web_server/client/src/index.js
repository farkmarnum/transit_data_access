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


/// React HELPER FUNCTIONS ///
function processData(data) {
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
  for (let tripHash in data.trips) {
    const trip = data.trips[tripHash]
    for (let elem of Object.entries(trip.arrivals)) {
      const stationHash = elem[0]
          , arrivalTime = elem[1]
          , finalStation = trip.branch.finalStation
      try {
        if (!data.stations[stationHash].arrivals[finalStation]) {
          data.stations[stationHash].arrivals[finalStation] = {}
        }
      } catch {
        console.debug(`ERROR on stationHash for trip.arrivals: ${trip.arrivals} and elem: ${elem}`)
      }
      try {
        data.stations[stationHash].arrivals[finalStation][arrivalTime] = tripHash
        data.routes[trip.branch.routeHash.toString()].finalStations.add(finalStation) // TODO figure out why each routeHash in data.routes is a string...
      } catch (err) {
        console.debug(`ERROR: ${trip.branch.routeHash} not in data.routes`)
      }
    }
  }

  return data
}

function dataReceivedMsg(timestamp) {
  return `{"type": "data_received", "last_successful_timestamp": "${timestamp}"}`
}

function requestFullMsg(error='none') {
  return `{"type": "request_full", "error": "${error}"}`
}

/// REACT COMPONENTS ///
function StationName(props) {
  return (
    <span className='station-name'>
      {props.name}
    </span>
  )
}

function ArrivalTime(props) {
  return (
    <span className={"arrival-time " + props.formattingClasses}>
      {props.time}
    </span>
  )
}

function RouteArrivals(props) {
  if (props.selectedRoute && props.selectedFinalStation) {
    return (
      <div className='stations'>
        <code>*Known Issue: some stations are out of order</code>
        {
          props.selectedRoute.stations.map((stationHash, i) => {
            // Filter arrivals to leave only those on a given route that haven't already happened yet
            const now = Date.now() / 1000
            const arrivals = props.data.stations[stationHash].arrivals[props.selectedFinalStation]
            if(!arrivals) return null

            let arrivalsForRoute = Object.keys(arrivals).filter(arrivalTime => {
              let tripHash = arrivals[arrivalTime]
              if(!tripHash || !props.data.trips[tripHash]) {
                console.error(`${tripHash} not found?`)
                return false
              }
              let routeHash = props.data.trips[tripHash].branch.routeHash
              return (
                (routeHash === props.selectedRouteHash) &&
                (arrivalTime - now > -30)
              )
            })
            arrivalsForRoute = arrivalsForRoute.sort().slice(0, 3)

            // convert each timeDiff (# of secs) to a text representation (30s, 4m, 12:34p, etc), and a styling based on how soon it is
            let arrivalTimeDiffsWithFormatting = arrivalsForRoute.map(arrivalTimeStr => { // TODO: why is this a string
              let arrivalTime = parseInt(arrivalTimeStr)
              let timeDiff = Math.floor(arrivalTime - now)
              let outList = []
              if (timeDiff < 15) {
                outList =  ["now", "very-soon"]
              } else if (timeDiff < 60) {
                outList =  [Math.floor(timeDiff) + "s", "very-soon"]
              } else if (timeDiff < 20 * 60) {
                outList =  [Math.floor(timeDiff / 60) + "m", "soon"]
              } else {
                outList =  [dateFormat(new Date(arrivalTime * 1000), 'h:MMt'), ""]
              }

              let tripHash = arrivals[arrivalTimeStr]
              if (props.updatedTrips.has(tripHash)) {
                outList[1] = outList[1] + " updated"
              }
              return outList
            })
            /// TODO: remove dupplicate "now" entries...

            if (arrivalTimeDiffsWithFormatting.length === 0) return null
            return (
              <div className='station' key={i}>
                <StationName name={props.data.stations[stationHash].name} />
                {
                  arrivalTimeDiffsWithFormatting.map((timeDiffWithFormatting, i) => {
                    let[timeDiff, formattingClasses] = timeDiffWithFormatting
                    return (
                      <ArrivalTime time={timeDiff} formattingClasses={formattingClasses} key={i} />
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
    this.finalStationClicked.bind(this)
    this.routeClicked.bind(this)
  }

  routeClicked(routeHash) {
    const newRouteHash = (routeHash === this.state.selectedRouteHash) ? null : routeHash
    const finalStations = (newRouteHash === null) ? null : this.props.data.routes[newRouteHash].finalStations
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
    if (this.props.data === null) {
      return "Waiting for data..."
    }
    if (!this.props.data.routes) {
      return "Routes not found"
    }

    const routeInfos = Object.entries(this.props.data.routes).map((elem, i) => {
      let routeHash = parseInt(elem[0])
      let routeInfo = elem[1]
      let routeName = this.props.data.routeNameLookup[routeHash]
      return [routeName, routeInfo, routeHash, i]
    })

    const selectedRoute = this.props.data.routes[this.state.selectedRouteHash]
    const selectedRouteName = this.props.data.routeNameLookup[this.state.selectedRouteHash]

    let finalStationsRender
    if (this.state.finalStations !== null && this.state.finalStations.size > 0) {
      finalStationsRender = [...this.state.finalStations].map((stationHash, i) => {
        return (
          <button
            className={"final-station " + ((this.state.selectedFinalStation === stationHash) ? "selected" : "")}
            onClick={() => this.finalStationClicked(stationHash)}
            key={i}
          >
            {
              "to " + this.props.data.stations[stationHash].name
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
        <h5> Arrivals by Route </h5>
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
                  onClick={() => this.routeClicked(routeHash)}
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
          updatedTrips={this.props.updatedTrips}
          data={this.props.data}
        />
      </div>
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
    const hostname = window.location.hostname
    const wsPort = process.env.WEBSOCKET_SERVER_PORT || 8000
    const wsURL = `ws://${hostname}:${wsPort}/?unique_id=${uniqueId}`
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
            console.debug(`this.state.lastSuccessfulTimestamp = ${this.state.lastSuccessfulTimestamp}, timestamp_from = ${parsed.timestamp_from}`)
          }
        }
      } else if (typeof data === 'object') {
        // received an object -- this should be either data_full or data_update
        if (data.size !== upcomingMessageBinaryLength) {
          console.debug('data.size - upcomingMessageBinaryLength is a difference of: ', data.size - upcomingMessageBinaryLength)
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
        console.debug('DATA RECEIVED TYPE = ', data)
      }
    }
  }


  updateRealtimeData(update) {
    let updatedTrips = new Set()
    let data = this.state.unprocessedData

    // Added arrivals:
    let added = update.arrivals.added
    Object.keys(added).forEach(tripHash => { // tripHash := String
      updatedTrips.add(tripHash)
      Object.entries(added[tripHash].arrival).forEach(elem => {
        let stationArrival = elem[1]
        data.trips[tripHash].arrivals[stationArrival.stationHash] = stationArrival.arrivalTime
        if (!stationArrival.stationHash || !stationArrival.arrivalTime) console.debug(stationArrival)
      })
    })

    // Deleted arrivals:
    let deleted = update.arrivals.deleted.tripStationDict
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

    // Modified arrivals:
    let modified = update.arrivals.modified
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

    // Added trips:
    added = update.trips.added
    added.forEach(obj => {
      let tripHash = obj.tripHash
        , trip = obj.info
      data.trips[tripHash] = trip

      for (let elem of Object.entries(obj.info.arrivals)) {
        const stationHash = elem[0]
            , arrivalTime = elem[1]
        try {
          if (!data.stations[stationHash].arrivals[arrivalTime]) {
            data.stations[stationHash].arrivals[arrivalTime] = {}
          }
        } catch (err) {
          console.log('data.stations[stationHash].arrivals[arrivalTime] failed with', err)
        }
        try {
          data.stations[stationHash].arrivals[arrivalTime][trip.branch.finalStation] = tripHash
        } catch (err) {
          console.error('data.stations[stationHash].arrivals[arrivalTime][trip.branch.finalStation failed with', err)
        }
      }
    })

    // Deleted trips:
    deleted = update.trips.deleted
    deleted.forEach(tripHash => {
      delete data.trips[tripHash]
    })

    // Trips with modified branches:
    modified = update.branch
    Object.keys(modified).forEach(tripHash => {
      data.trips[tripHash].branch = modified[tripHash]
    })

    /// TODO: modified status

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
    this.forceUpdateInterval = setInterval(() => this.setState({ time: Date.now() }), 1 * 1000);
  }
  componentWillUnmount() {
    clearInterval(this.forceUpdateInterval);
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
    this.setState({
      unprocessedData: unprocessedData,
      processedData: processedData
    })
    this.flashDataStatus()
  }
  loadUpdate(raw) {
    const update = DataUpdate.decode(raw)
    this.updateRealtimeData(update)
  }


  decodeZippedProto(compressedBlob) {
    var fileReader = new FileReader();
    fileReader.onload = (event) => {
        const decompressed = pako.inflate(event.target.result)
        if (upcomingMessageType === DATA_FULL) this.loadFull(decompressed)
        else if (upcomingMessageType === DATA_UPDATE) this.loadUpdate(decompressed)
        else console.error("upcomingMessageType not valid")
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
              <RouteList data={ this.state.processedData } updatedTrips={ this.state.updatedTrips }/>
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
