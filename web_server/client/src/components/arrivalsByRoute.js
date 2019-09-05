import React from 'react'
import { ArrivalTime } from './baseComponents.js'
import { timeDiffsFromBranchArrivals, toColorCode } from './sharedFunctions.js'

function RouteStationName(props) {
  return (
    <span className='route-station-name'>
      {props.name}
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
          const routeName = elem[0]
              , routeInfo = elem[1]
              , routeHash = elem[2]
              , i         = elem[3]

          const routeColor = toColorCode(routeInfo.color)
          // ^^ TODO: move this logic into processData to be DRY
          let style
          if (routeHash === props.selectedRouteHash) {
            style = {backgroundColor: routeColor, color: 'white'}
          } else {
            style = {color: routeColor, backgroundColor: 'inherit'}
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
    </React.Fragment>
  )
}

function FinalStationsRender(props) {
  let output
    , station
  if (props.selectedRouteHash == null) {
    output = "Click a route to see arrivals!"
  } else if (props.finalStations !== null && props.finalStations.size > 0) {
    output = [...props.finalStations].map((stationHash, i) => {
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

    return (
      <React.Fragment>
      <div className="arrivals-by-route-header">
        <h5>
          Arrivals By Route
        </h5>
        <div className="route-names-and-final-stations">
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