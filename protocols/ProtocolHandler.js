const { protocolMapping } = require('./index.js')

const { print, unpack } = require('../helpers')

class ProtocolHanlder {
  constructor ({ ws }) {
    this.ws = ws
    this.protocol = null
    this._init()
  }

  _init () {
    // connection errors are handle on ws.on('error')
    this.ws.on('open', () => this._openDecorator({ ws: this.ws }))

    this.ws.on('close', (code, reason) => this._dispatch('_closeDecorator', { ws: this.ws, code, reason }))

    this.ws.on('error', (err) => this._dispatch('_errorDecorator', { ws: this.ws, err }))

    this.ws.on('message', (msg) => this._dispatch('_messageDecorator', { ws: this.ws, msg }))
  }

  _dispatch (action, payload) {
    if (action === '_messageDecorator') {
      let msg = unpack(payload.msg)

      if (msg.message && msg.message === 'job-info') {
        const protocol = msg.job && msg.job.protocol
        if (protocolMapping.has(protocol)) {
          const Protocol = protocolMapping.get(protocol)
          this.protocol = new Protocol({ ws: this.ws })
          this.protocol.job = { ...msg.job }
        }

        this._messageDecorator(payload)
        return
      }
    }

    if (!this.protocol) {
      this.ws.close(1000, 'Protocol is not specified.')
      return
    }

    this[action](payload)
  }

  _openDecorator ({ ws }) {
    console.log('Connection opened.')
  }

  _closeDecorator ({ ws, code, reason }) {
    console.log(`Connection closed with code ${code} and reason ${reason}.`)
    this.protocol.handleClose({ ws, code, reason })
  }

  _errorDecorator ({ ws, err }) {
    console.log(err)
    this.protocol.handleError({ ws, err })
  }

  _messageDecorator ({ ws, msg }) {
    print(`Message: ${msg}`)
    msg = unpack(msg)
    this.protocol.handleMessage({ ws, msg })
  }
}

module.exports = ProtocolHanlder
