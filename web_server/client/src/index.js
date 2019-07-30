import React from 'react';
import ReactDOM from 'react-dom';
import './normalize.css';
import './skeleton.css';
import './index.css';
const utils = require('./utils');
const WebSocket = require('ws');
// const crypto = require('crypto');

const parserHostname = process.env.PARSER_HOSTNAME;
const parserPort = process.env.PARSER_PORT

class Main extends React.Component {
	constructor(props) {
		super(props)
		this.state = ({

		})
	}
	render() {
		return (
			<div>
				{utils.formatBytes(44444444)}
			</div>
		)
	}
}

// ========================================

ReactDOM.render(
  <Main />,
  document.getElementById('root')
);
