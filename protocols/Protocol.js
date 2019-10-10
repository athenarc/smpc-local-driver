const Client = require('../ClientSMPC')

class Protocol {
  constructor ({ ws }) {
    if (new.target === Protocol) {
      throw new TypeError('Cannot construct abstract Protocol instances directly')
    }

    this.ws = ws
    this.client = new Client(process.env.ID)
    this.job = null
    this._init()
    this._registerToClient()
  }

  _init () {
    // connection errors are handle on ws.on('error')
    this.ws.on('open', () => this.handleOpen({ ws: this.ws }))

    this.ws.on('close', (code, reason) => this.handleClose({ ws: this.ws, code, reason }))

    this.ws.on('error', (err) => this.handleError({ ws: this.ws, err }))

    this.ws.on('message', (msg) => this.handleMessage({ ws: this.ws, msg }))
  }

  _registerToClient () {
    this.client.on('error', msg => this.handleClientError(msg))
    this.client.on('exit', msg => this.handleClientExit(msg))
  }

  /* Abstract Methods */
  handleOpen ({ ws }) {
    throw new Error('handleOpen: Implementation Missing!')
  }

  handleClose ({ ws, code, reason }) {
    throw new Error('handleClose: Implementation Missing!')
  }

  handleError ({ ws, err }) {
    throw new Error('handleError: Implementation Missing!')
  }

  handleClientError (msg) {
    throw new Error('handleClientError: Implementation Missing!')
  }

  handleClientExit (msg) {
    throw new Error('handleClientExit: Implementation Missing!')
  }

  handleMessage ({ ws, msg }) {
    throw new Error('handleMessage: Implementation Missing!')
  }
}

module.exports = Protocol
