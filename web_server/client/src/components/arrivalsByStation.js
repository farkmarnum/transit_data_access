import React from 'react'
import {
  ArrivalTime,
  RouteIcon
} from './baseComponents.js'
import {
  timeDiffsFromBranchArrivals,
} from './sharedFunctions.js'

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
  const finalStation = props.data.stations[props.finalStationHash]
  const branchArrivals = props.station.arrivals[props.routeHash][props.finalStationHash]
  if (branchArrivals == null || Object.keys(branchArrivals).length === 0) {
    return "no arrivals"
  }
  // const direction = Object.keys(branchArrivals)[0].direction
  const timeDiffsWithFormatting = timeDiffsFromBranchArrivals(
    branchArrivals,
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
  // const formattedRoute = formatRouteName(routeName)

  return (
    <div className="station-arrivals">
      <span className="station-route-name-outer">
        <RouteIcon
          routeName={routeName}
          routeColor={props.routeColor}
          clickable={false}
        />
      </span>
      <span className="station-name-and-borough">
        <div className="final-station-name">{ finalStation.name }</div>
        <div className="borough-label">{ finalStation.borough }</div>
      </span>
      <span className="station-route-arrivals">
        { formattedArrivals }
      </span>
    </div>
  )
}

function Station(props) {
  const station = props.data.stations[props.stationHash]
  let arrival
    , key
    , routeColor
  return (
    <div className="station">
      <div className="station-header">
        <span className="station-name">{ station.name }</span>
        {/*<span className="borough-label">{ station.borough }</span>*/}
      </div>
      <div className="station-routes">
        {
          Object.keys(station.arrivals).map((routeHash, i) => {
            arrival = station.arrivals[routeHash]
            if (Object.keys(arrival).length === 0) {
              return "no arrivals"
            }
            return Object.keys(arrival).map((finalStationHash, j) => {
              key = i * 1000 + j
              routeColor = (
                "#" +
                ("00"+(Number(props.data.routes[routeHash].color).toString(16))).slice(-6)
              )
              return (
                <StationRouteArrivals
                  key={key}
                  data={props.data}
                  routeHash={routeHash}
                  finalStationHash={finalStationHash}
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


const initResultLimit = 6;
const resultLimitIncrease = 6;

export class ArrivalsByStation extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      searchText: "",
      resultLimit: initResultLimit
    }
  }

  updateSearchText = (searchText) => {
    let trimmedText = searchText.trim().toLowerCase()
    this.setState({
      searchText: trimmedText,
      resultLimit: initResultLimit
    })
  }

  increaseResultLimit = () => {
    this.setState({
      resultLimit: this.state.resultLimit + resultLimitIncrease
    })
  }

  render() {
    let stationList = []
    if (this.state.searchText !== "") {
      Object.keys(this.props.data.stations).forEach((stationHash, i) => {
        let name = this.props.data.stations[stationHash].name
        if (name.toLowerCase().indexOf(this.state.searchText) >= 0) {
          stationList.push(
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

    let showMore = ""
    if (stationList.length > this.state.resultLimit) {
      showMore = <div className="show-more" onClick={this.increaseResultLimit}>show more results</div>
    }

    return (
      <div>
        <div className="arrivals-by-station">
          <div className="arrivals-by-station-header">
            <h5>
              Arrivals By Station
            </h5>
            <Search
              placeholder='start typing a station name...'
              onInput={this.updateSearchText}
            />
          </div>
          { stationList.slice(0, this.state.resultLimit) }
        </div>
        { showMore }
      </div>
    )
  }
}
