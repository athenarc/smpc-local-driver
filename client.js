require('dotenv').config()

const _ = require('lodash')
const WebSocket = require('ws')
const Client = require('./Client')
const { print, pack, unpack } = require('./helpers')

const PORT = process.env.PORT || 3004

if (_.isEmpty(process.env.SMPC_ENGINE)) {
  throw new Error('SMPC Engine absolute path not defined!')
}

if (_.isEmpty(process.env.ID)) {
  throw new Error('Client ID not defined!')
}

const wss = new WebSocket.Server({ port: PORT })
const client = new Client(process.env.ID)

wss.on('connection', (ws) => {
  print('Connection Accepted!')

  ws.on('message', (data) => {
    print(`Message: ${data}`)
    data = unpack(data)
    if (data.message === 'import') {
      client.run()

      client.on('exit', () => {
        ws.send(pack({ message: 'exit', client: { id: client.id } }))
      })
    }
  })
})
