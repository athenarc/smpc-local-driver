const Client = require('../ClientSMPC')

class Protocol {
  constructor ({ ws }) {
    if (new.target === Protocol) {
      throw new TypeError('Cannot construct abstract Protocol instances directly')
    }

    this.client = new Client(process.env.ID)
    this.job = null
    this.ws = ws
    this._registerToClient()
  }

  _registerToClient () {
    this.client.on('error', msg => this._clientErrorDecorator(msg))
    this.client.on('exit', msg => this._clientExitDecorator(msg))
  }

  /* Decorators */
  _clientErrorDecorator (msg) {
    this.handleClientError(msg)
  }

  _clientExitDecorator (msg) {
    this.handleClientExit(msg)
  }

  /* Abstract Methods */
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
