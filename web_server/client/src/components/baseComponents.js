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
    , selected = props.selected ? " selected" : ""
    , clickable = props.clickable ? " clickable" : ""

  if ("X" === name.slice(-1)) {
    // DIAMOND
    name = name.slice(0, 1)
    return (
      <button
        className={"route-diamond-outer" + selected + clickable}
        style={{color: txtColor, borderColor: bgColor}}
        onClick={props.clickable ? props.routeClicked : noop}
      >
        { name }
        <div
          className={"route-diamond-inner" + selected}
          style={{backgroundColor: bgColor}}
        ></div>
      </button>
    )
  } else {
    // CIRCLE
    const style = {
      color: txtColor,
      backgroundColor: bgColor,
      borderColor: bgColor
    }
    if (name.length > 1) {
      style.letterSpacing = "-0.1rem"
    }
    return (
      <button
        className={"route-circle" + selected + clickable}
        style={style}
        onClick={props.clickable ? props.routeClicked : noop}
      >
        { name }
      </button>
    )
  }

}