class Protocol {
  constructor ({ ws }) {
    if (new.target === Protocol) {
      throw new TypeError('Cannot construct abstract Protocol instances directly')
    }

    this.ws = ws
    this._init()
  }

  _init () {
    // connection errors are handle on ws.on('error')
    this.ws.on('open', () => this.handleOpen({ ws: this.ws }))

    this.ws.on('close', (code, reason) => this.handleClose({ ws: this.ws, code, reason }))

    this.ws.on('error', (err) => this.handleError({ ws: this.ws, err }))

    this.ws.on('message', (msg) => this.handleMessage({ ws: this.ws, msg }))
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

  handleMessage ({ ws, msg }) {
    throw new Error('handleMessage: Implementation Missing!')
  }
}

module.exports = Protocol
