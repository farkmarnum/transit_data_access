import React from 'react'
import {
  ArrivalTime,
  RouteIcon
} from './baseComponents.js'
import {
  timeDiffsFromBranchArrivals,
  toColorCode
} from './sharedFunctions.js'

function RouteStationName(props) {
  return (
    <span className='route-station-name'>
      {props.name}
    </span>
  )
}

function RouteArrivals(props) {
  if (props.selectedRoute && props.selectedFinalStation) {
    let tripHashes = new Set()
    props.selectedRoute.stations.forEach((stationHash, i) => {
      const station = props.data.stations[stationHash]
      let arrivalsForBranch
      try {
        arrivalsForBranch = station.arrivals[props.selectedRouteHash][props.selectedFinalStation]
        if(arrivalsForBranch) {
          Object.values(arrivalsForBranch).forEach((tripHash) => {tripHashes.add(tripHash)})
        }
      } catch {
      }
    })
    let longestTripHash
      , currLength
    let longestLenth = 0
    tripHashes.forEach((tripHash) => {
      currLength = Object.keys(props.data.trips[tripHash].arrivals).length
      if (currLength > longestLenth) {
        longestTripHash = tripHash
        longestLenth = currLength
      }
    })

    const lthArrivals = props.data.trips[longestTripHash].arrivals
    const sortedStationHashes = Object.keys(lthArrivals).sort(function(hash1, hash2) {return lthArrivals[hash1] - lthArrivals[hash2]});

    return (
      <div className='route-stations'>
        {
          // Filter arrivals to leave only those on a given route that haven't already happened yet
          sortedStationHashes.map((stationHash, i) => {
            const station = props.data.stations[stationHash]
            let arrivalsForBranch
            try {
              arrivalsForBranch = station.arrivals[props.selectedRouteHash][props.selectedFinalStation]
              if(!arrivalsForBranch) {
                return null
              }
            } catch {
              return null
            }

            // convert each timeDiff (# of secs) to a text representation (30s, 4m, 12:34p, etc), and a styling based on how soon it is
            let arrivalTimeDiffsWithFormatting = timeDiffsFromBranchArrivals(arrivalsForBranch, props.updatedTrips)

            if (arrivalTimeDiffsWithFormatting.length === 0) return null
            return (
              <div className='route-station' key={i}>
                <div className="route-station-name-outer">
                  <span className="small-dot" style={{backgroundColor: toColorCode(props.selectedRoute.color)}}></span>
                  <RouteStationName name={props.data.stations[stationHash].name} />
                </div>
                <span className="arrival-times">
                  {
                    arrivalTimeDiffsWithFormatting.map((timeDiffWithFormatting, i) => {
                      let[timeDiff, formattingClasses] = timeDiffWithFormatting
                      return (
                        <ArrivalTime time={timeDiff} formattingClasses={formattingClasses} key={i} />
                      )
                    })
                  }
                </span>
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
    <React.Fragment>
      {
        props.routeInfos.sort().map(elem => {
          let routeName  = elem[0]
            , routeInfo  = elem[1]
            , routeHash  = elem[2]
            , i          = elem[3]
          return (
            <RouteIcon
              routeName={routeName}
              routeColor={toColorCode(routeInfo.color)}
              selected={routeHash === props.selectedRouteHash}
              key={i}
              clickable={true}
              routeClicked={() => props.routeClicked(routeHash)}
            />
          )
        })
      }
    </React.Fragment>
  )
}

function FinalStationsForDirection(props) {
  let station
  return (
    <React.Fragment>
      <div className="route-arrivals-direction-title">
        {props.direction ? "North" : "South"}-bound
      </div>
      {
        [...props.directionStations].map((stationHash, i) => {
          station = props.stations[stationHash]
          return (
            <button
              className={"route-final-station " + ((props.selectedFinalStation === stationHash) ? "selected" : "")}
              onClick={() => props.finalStationClicked(stationHash)}
              key={i}
            >
            <span className="final-station-name">to { station.name }</span>
            <span className="borough-label">{ station.borough }</span>
            </button>
          )
        })
      }
    </React.Fragment>
  )
}

function FinalStations(props) {
  let output = []
  if (props.selectedRouteHash == null) {
    output = "Click a route to see arrivals!"
  } else if (props.finalStations !== null && props.finalStations.size > 0) {
    let northStations = []
      , southStations = []
    props.finalStations.forEach((finalStationHash, i) => {
      let direction = props.stations[finalStationHash].finalStationDirection
      if (direction === true) {
        northStations.push(finalStationHash)
      } else if (direction === false) {
        southStations.push(finalStationHash)
      } else {
        console.error(props.stations[finalStationHash], "does not have .finalStationDirection")
      }
    })

    if (northStations.length > 0) {
      output.push(
        <FinalStationsForDirection
          key={0}
          directionStations={northStations}
          direction={true}
          selectedFinalStation={props.selectedFinalStation}
          finalStationClicked={props.finalStationClicked}
          stations={props.stations}
        />
      )
    }
    if (southStations.length > 0) {
      output.push(
        <FinalStationsForDirection
          key={1}
          directionStations={southStations}
          direction={false}
          selectedFinalStation={props.selectedFinalStation}
          finalStationClicked={props.finalStationClicked}
          stations={props.stations}
        />
      )
    }

  } else {
    if (props.selectedRouteHash !== null) {
      output = "No trains currently running on this route."
    } else {
      output = null
    }
  }
  return (
    <div className="route-final-stations">
      { output }
    </div>
  )
}


export class ArrivalsByRoute extends React.Component {
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

    // let selectedRouteDisplay = ""
    // if (selectedRoute) {
    //   selectedRouteDisplay = (
    //     <RouteIcon
    //       routeName={this.props.data.routeNameLookup[this.state.selectedRouteHash]}
    //       routeColor={toColorCode(selectedRoute.color)}
    //       selected={true}
    //       clickable={false}
    //     />
    //   )
    // }
    return (
      <React.Fragment>
      <div className="arrivals-by-route-header">
        <h5 style={{width: "50%", display: "inline-block"}}>
          Arrivals By Route
        </h5>
        { /*selectedRouteDisplay*/ }
        <div className="route-names-and-final-stations">
          <RouteNameList
            routeInfos={routeInfos}
            selectedRouteHash={this.state.selectedRouteHash}
            routeClicked={this.routeClicked}
          />
          <FinalStations
            finalStations={this.state.finalStations}
            selectedFinalStation={this.state.selectedFinalStation}
            selectedRouteHash={this.state.selectedRouteHash}
            stations={this.props.data.stations}
            finalStationClicked={this.finalStationClicked}
          />
        </div>
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
      </React.Fragment>
    )
  }
}