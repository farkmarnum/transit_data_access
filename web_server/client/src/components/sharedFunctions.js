const dateFormat = require('dateformat')

export function timeDiffsFromBranchArrivals(arrivalsForBranch, updatedTrips) {
  // Note: arrivalsForBranch should be from arrivals[routeHash][finalStation] for some routeHash and finalStation
  const now = Date.now() / 1000
  arrivalsForBranch = Object.keys(arrivalsForBranch).filter(arrivalTime => {
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

export function toColorCode(str) {
  return "#" + ("00"+(Number(str).toString(16))).slice(-6)
}