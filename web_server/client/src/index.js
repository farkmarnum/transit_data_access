import React from 'react'
import ReactDOM from 'react-dom'
import './normalize.css'
import './skeleton.css'
import './index.css'
import { sleep } from './utils.js'

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

function devLog(entry) {
  if(process.env.NODE_ENV === 'development') {
    console.log(entry)
  }
}


/// OBJECTS (initialzed later)
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

  // create a set of finalStations for each route
  for (let routeHash in data.routes) {
    data.routes[routeHash].finalStations = new Set()
  }

  // add an arrivals list for each station and gather station_name & stationhash into two complimentary mappings
  data.stationNameFromHash = {}
  data.stationNames = []
  for (let stationHash in data.stations) {
    data.stations[stationHash].arrivals = {}
    data.stationNameFromHash[stationHash] = data.stations[stationHash].name
    data.stationNames.push({
      name: data.stations[stationHash].name,
      stationHash: stationHash
    })
  }
  // populate the arrivals list for each station in props.stations
  for (let tripHash in data.trips) {
    const trip = data.trips[tripHash]
    for (let elem of Object.entries(trip.arrivals)) {
      const stationHash = elem[0]
          , arrivalTime = elem[1]
          , finalStation = trip.branch.finalStation
          , routeHash = trip.branch.routeHash.toString()
          // TODO figure out why each routeHash in data.routes is a string...

      // create the nexted dicts if necessary:
      try {
        if (!data.stations[stationHash].arrivals[routeHash]) {
          data.stations[stationHash].arrivals[routeHash] = {}
        }
        if (!data.stations[stationHash].arrivals[routeHash][finalStation]) {
          data.stations[stationHash].arrivals[routeHash][finalStation] = {}
        }
      } catch { devLog(`ERROR on stationHash for trip.arrivals: ${trip.arrivals} and elem: ${elem}`) }

      // add this arrival to the station 
      data.stations[stationHash].arrivals[routeHash][finalStation][arrivalTime] = tripHash
      // add this arrival's finalStation to this route's 'finalStations' Set
      try {
        data.routes[trip.branch.routeHash].finalStations.add(finalStation)
      } catch (err) {
        devLog(`ERROR: ${trip.branch.routeHash} not in data.routes`)
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

function timeDiffsFromBranchArrivals(arrivalsForBranch, updatedTrips) {
  // Note: arrivalsForBranch should be from arrivals[routeHash][finalStation] for some routeHash and finalStation
  const now = Date.now() / 1000
  arrivalsForBranch = Object.keys(arrivalsForBranch).filter(arrivalTime => {
    // let tripHash = arrivalsForBranch[arrivalTime]
    // if(!tripHash || !props.data.trips[tripHash]) {
    //   console.error(`${tripHash} not found?`)
    //   return false
    // }
    return (arrivalTime - now > -30)
  })
  // slice to get just the three most recent arrivals
  arrivalsForBranch = arrivalsForBranch.sort().slice(0, 3)

  return arrivalsForBranch.map(arrivalTimeStr => { // TODO: why is this a string
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

    let tripHash = arrivalsForBranch[arrivalTimeStr]
    if (updatedTrips.has(tripHash)) {
      outList[1] = outList[1] + " updated"
    }
    return outList
  }) /// TODO: remove duplicate "now" entries...
}


/// REACT COMPONENTS ///
function RouteStationName(props) {
  return (
    <span className='route-station-name'>
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
      <div className='route-stations'>
        <code>*Known Issue: some stations are out of order</code>
        {
          // Filter arrivals to leave only those on a given route that haven't already happened yet
          props.selectedRoute.stations.map((stationHash, i) => {
            const station = props.data.stations[stationHash]
            let arrivalsForBranch
            try {
              arrivalsForBranch = station.arrivals[props.selectedRouteHash][props.selectedFinalStation]
            if(!arrivalsForBranch) return null
            } catch {
              return null
            }

            // convert each timeDiff (# of secs) to a text representation (30s, 4m, 12:34p, etc), and a styling based on how soon it is
            let arrivalTimeDiffsWithFormatting = timeDiffsFromBranchArrivals(arrivalsForBranch, props.updatedTrips)
            if (arrivalTimeDiffsWithFormatting.length === 0) return null
            return (
              <div className='route-station' key={i}>
                <RouteStationName name={props.data.stations[stationHash].name} />
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


function RouteNameList(props) {
  return (
    <div className="route-name-list">
      {
        props.routeInfos.sort().map(elem => {
          const routeName = elem[0]
              , routeInfo = elem[1]
              , routeHash = elem[2]
              , i         = elem[3]

          const routeColor = "#" + ("00"+(Number(routeInfo.color).toString(16))).slice(-6)
          // ^^ TODO: move this logic into processData to be DRY
          let style
          if (routeHash === props.selectedRouteHash) {
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
              onClick={() => props.routeClicked(routeHash)}
            >
              { routeName }
            </button>
          )
        })
      }
    </div>
  )
}

function FinalStationsRender(props) {
  let output
  if (props.selectedRouteHash == null) {
    output = "Click a route to see arrivals!"
  } else if (props.finalStations !== null && props.finalStations.size > 0) {
    output = [...props.finalStations].map((stationHash, i) => {
      return (
        <button
          className={"route-final-station " + ((props.selectedFinalStation === stationHash) ? "selected" : "")}
          onClick={() => props.finalStationClicked(stationHash)}
          key={i}
        >
          { "to " + props.stations[stationHash].name }
        </button>
      )
    })
  } else {
    if (props.selectedRouteHash !== null) {
      output = "No trains currently running on this route."
    } else {
      output = props.selectedRouteHash
    }
  }
  return (
    <div className="route-final-stations">
      { output }
    </div>
  )
}

class ArrivalsByRoute extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      selectedRouteHash: null,
      selectedFinalStation: null,
      finalStations: null
    }
  }

  routeClicked = (routeHash) => {
    const newRouteHash = (routeHash === this.state.selectedRouteHash) ? null : routeHash
    const finalStations = (newRouteHash === null) ? null : this.props.data.routes[newRouteHash].finalStations
    this.setState({
      selectedRouteHash: newRouteHash,
      finalStations: finalStations,
      selectedFinalStation: null
    })
  }

  finalStationClicked = (stationHash) => {
    this.setState({
      selectedFinalStation: (stationHash === this.state.selectedFinalStation) ? null : stationHash
    })
  }

  render() {
    const routeInfos = Object.entries(this.props.data.routes).map((elem, i) => {
      let routeHash = parseInt(elem[0])
      let routeInfo = elem[1]
      let routeName = this.props.data.routeNameLookup[routeHash]
      return [routeName, routeInfo, routeHash, i]
    })

    const selectedRoute = this.props.data.routes[this.state.selectedRouteHash]
    const selectedRouteName = this.props.data.routeNameLookup[this.state.selectedRouteHash]

    return (
      <React.Fragment>
        <h5>
          Arrivals By Route
        </h5>
        <RouteNameList
          routeInfos={routeInfos}
          selectedRouteHash={this.state.selectedRouteHash}
          routeClicked={this.routeClicked}
        />
        <FinalStationsRender
          finalStations={this.state.finalStations}
          selectedFinalStation={this.state.selectedFinalStation}
          selectedRouteHash={this.state.selectedRouteHash}
          stations={this.props.data.stations}
          finalStationClicked={this.finalStationClicked}
        />
        <RouteArrivals
          className="route-arrivals"
          selectedRoute={selectedRoute}
          selectedRouteHash={this.state.selectedRouteHash}
          selectedRouteName={selectedRouteName}
          selectedFinalStation={this.state.selectedFinalStation}
          updatedTrips={this.props.updatedTrips}
          data={this.props.data}
        />
      </React.Fragment>
    )
  }
}


class Search extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      inputValue: ''
    }
  }

  updateInputValue(evt) {
    this.setState({
      inputValue: evt.target.value
    })
  }

  render() {
    return (
      <input
        className="u-full-width"
        onInput={
          evt => {
            this.updateInputValue(evt)
            this.props.onInput(evt.target.value)
          }
        }
        placeholder={this.props.placeholder}
     />
    )
  }
}

function StationRouteArrivals(props) {
  const routeName = props.data.routeNameLookup[props.routeHash]
  const finalStationName = props.data.stationNameFromHash[props.finalStation]
  const timeDiffsWithFormatting = timeDiffsFromBranchArrivals(
    props.station.arrivals[props.routeHash][props.finalStation],
    props.updatedTrips
  )
  const formattedArrivals = timeDiffsWithFormatting.map(
    (timeDiffWithFormatting, i) => {
      let[timeDiff, formattingClasses] = timeDiffWithFormatting
      return (
        <ArrivalTime time={timeDiff} formattingClasses={formattingClasses} key={i} />
      )
    }
  )

  return (
    <div className="station-arrivals">
      <span className="station-branch">
        <span className="station-route-name" style={{color: "white", backgroundColor: props.routeColor}}>
          { routeName }
        </span>
        to { finalStationName }:
      </span>
      <span className="station-route-arrivals">
        { formattedArrivals }
      </span>
    </div>
  )
}

function Station(props) {
  const station = props.data.stations[props.stationHash]
  return (
    <div className="station">
      <div className="station-name">
        { station.name }
      </div>
      <div className="station-routes">
        {
          Object.keys(station.arrivals).map((routeHash, i) => {
            return Object.keys(station.arrivals[routeHash]).map((finalStation, j) => {
              const key = i * 1000 + j
              const routeColor = (
                "#" +
                ("00"+(Number(props.data.routes[routeHash].color).toString(16))).slice(-6)
              )
              return (
                <StationRouteArrivals
                  key={key}
                  data={props.data}
                  routeHash={routeHash}
                  finalStation={finalStation}
                  routeColor={routeColor}
                  station={station}
                  updatedTrips={props.updatedTrips}
                />
              )
            })
          })
        }
      </div>
    </div>
  )
}

class ArrivalsByStation extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      searchText: ""
    }
  }

  updateSearchText = (searchText) => {
    let trimmedText = searchText.trim().toLowerCase()
    this.setState({
      searchText: (trimmedText.length > 1) ? trimmedText : ""
    })
  }

  render() {
    let stationList = ""
    if (this.state.searchText !== "") {
      stationList = this.props.data.stationNames.map((stationNameAndHash, i) => {
        let name = stationNameAndHash.name
          , stationHash = stationNameAndHash.stationHash
        if (name.toLowerCase().indexOf(this.state.searchText) >= 0) {
          return (
            <Station
              key={ i }
              data={ this.props.data }
              stationHash={ stationHash }
              updatedTrips={ this.props.updatedTrips }
            />
          )
        } else {
          return null
        }
      })
    }
    return (
      <div className="arrivals-by-station">
        <h5>
          Arrivals By Station
        </h5>
        <Search
          placeholder='start typing a station name...'
          onInput={this.updateSearchText}
        />
        { stationList }
      </div>
    )
  }
}


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
      return "Waiting for data..."
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

    // Added arrivals:
    let added = update.arrivals.added
    Object.keys(added).forEach(tripHash => { // tripHash := String
      updatedTrips.add(tripHash)
      Object.entries(added[tripHash].arrival).forEach(elem => {
        let stationArrival = elem[1]
        data.trips[tripHash].arrivals[stationArrival.stationHash] = stationArrival.arrivalTime
        if (!stationArrival.stationHash || !stationArrival.arrivalTime) devLog(stationArrival)
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

    // Trips with modified status:
    // TODO: double check this
    modified = update.status
    Object.keys(modified).forEach(tripHash => {
      data.trips[tripHash].status = modified[tripHash]
    })

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
            <div className="ten columns offset-by-one">
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
