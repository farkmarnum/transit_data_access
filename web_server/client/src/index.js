import React from 'react';
import ReactDOM from 'react-dom';
import './normalize.css';
import './skeleton.css';
import './index.css';
const utils = require('./utils');
// const WebSocket = require('ws');
// const crypto = require('crypto');

// const parserHostname = process.env.PARSER_HOSTNAME;
// const parserPort = process.env.PARSER_PORT

class Main extends React.Component {
  constructor(props) {
    super(props)
    this.state = ({
        data: null
    })
  }

  componentDidMount() {
    // Call our fetch function below once the component mounts
    this.callBackendAPI()
      .then(res => this.setState({ data: res.express }))
      .catch(err => console.log(err));
  }

  // Fetches our GET route from the Express server. (Note the route we are fetching matches the GET route from server.js
  async callBackendAPI() {
    const response = await fetch('/mta');
    const body = await response.json();

    if (response.status !== 200) {
      throw Error(body.message) 
    }
    return body;
  }

  render() {
    return (
      <div className="container">
        <div className="ten columns offset-by-two">
          {this.state.data}
          <br/>
          {utils.formatBytes(44444444)}
          blaaaa
        </div>
      </div>
    )
  }
}

// ========================================

ReactDOM.render(
  <Main />,
  document.getElementById('root')
);
