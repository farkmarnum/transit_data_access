import React from 'react'

const noop = () => {}

export function ArrivalTime(props) {
  return (
    <span className={"arrival-time " + props.formattingClasses}>
      {props.time}
    </span>
  )
}

export function RouteIcon(props) {
  let name = props.routeName
    , bgColor = props.routeColor
    , txtColor = "white" // TODO: figure out how to make this -> $brightWhite
    , selected
    , opacity
  if (!props.clickable || props.selected) {
    opacity = 1
  } else {
    opacity = 1
    // opacity = 0.5
  }

  if ("X" === name.slice(-1)) {
    // DIAMOND
    name = name.slice(0, 1)
    return (
      <button className="route-diamond-outer" style={{color: txtColor}} onClick={props.clickable ? props.routeClicked : noop}>
          { name }
        <div className="route-diamond-inner" style={{backgroundColor: bgColor, opacity: opacity}}></div>
      </button>
    )
  } else {
    // CIRCLE
    const style = {
      color: txtColor,
      backgroundColor: bgColor,
      opacity: opacity
    }
    if (props.selected) {
      selected = <div className="selected-route-circle"></div>
    } else {
      selected = ""
    }
    return (
      <button className={"route-circle"} style={style} onClick={props.clickable ? props.routeClicked : noop}>
          { name }
          { selected }
      </button>
    )
  }

}