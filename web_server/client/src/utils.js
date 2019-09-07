import Fuse from "fuse.js";

export function devLog(entry) {
  if(process.env.NODE_ENV === 'development') {
    console.log(entry)
  }
}

export function formatBytes(bytes, decimals) {
  if (bytes === 0) {
    return '0 Bytes';
  }
  var k = 1024;
  var dm = decimals <= 0 ? 0 : decimals || 2;
  var sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
  var i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

export function sleep(ms) {
	return new Promise(resolve => setTimeout(resolve, ms))
}


/// React HELPER FUNCTIONS ///
export function processData(data) {
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

  // add an arrivals list for each station
  for (let stationHash in data.stations) {
    data.stations[stationHash].arrivals = {}
  }
  // populate the arrivals list for each station in props.stations
  for (let tripHash in data.trips) {
    const trip = data.trips[tripHash]
    for (let elem of Object.entries(trip.arrivals)) {
      const stationHash = elem[0]
          , arrivalTime = elem[1]
          , finalStation = trip.branch.finalStation
          , routeHash = trip.branch.routeHash.toString()
          /// TODO figure out why each routeHash in data.routes is a string...

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
        devLog(trip)
      }

      // add the direction label to this trip's lastStation
      /// DEBUG
        // if(finalStation === 445920373) {
        //   console.log("445920373 is finalStation for ", trip)
        // }
      if (data.stations[finalStation].finalStationDirection && data.stations[finalStation].finalStationDirection !== trip.direction) {
        devLog(`WARNING: ${data.stations[finalStation].name} has multiple trips referencing it as finalStation w/ different trip directions...`)
      }
      data.stations[finalStation].finalStationDirection = trip.direction
    }
  }

  /// Generate Fuse for stations
  const options = {
    keys: ['station'],
    shouldSort: true,
    threshold: 0.4
  }
  const stationObjList = Object.keys(data.stations).map((stationHash) => {
    return {
      'station': data.stations[stationHash].name,
      'stationHash': stationHash
    }
  })
  data.stationSearch = new Fuse(stationObjList, options)

  return data
}

export function dataReceivedMsg(timestamp) {
  return `{"type": "data_received", "last_successful_timestamp": "${timestamp}"}`
}

export function requestFullMsg(error='none') {
  return `{"type": "request_full", "error": "${error}"}`
}
