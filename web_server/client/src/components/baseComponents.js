import React from 'react'

export function ArrivalTime(props) {
  return (
    <span className={"arrival-time " + props.formattingClasses}>
      {props.time}
    </span>
  )
}