require('dotenv').config()

const _ = require('lodash')
const WebSocket = require('ws')
const Client = require('./ClientSMPC')
const { print, pack, unpack } = require('./helpers')

const PORT = process.env.PORT || 3004

if (_.isEmpty(process.env.SMPC_ENGINE)) {
  throw new Error('SMPC Engine absolute path not defined!')
}

if (_.isEmpty(process.env.ID)) {
  throw new Error('Client ID not defined!')
}

const wss = new WebSocket.Server({ port: PORT, clientTracking: true })

const handleConnection = (ws) => {
  const client = new Client(process.env.ID)

  ws.on('close', (data) => {
    console.log('Connection Closed: ', data)
  })

  ws.on('error', (err) => {
    console.log(err)
  })

  ws.on('message', (data) => {
    print(`Message: ${data}`)
    data = unpack(data)

    if (data.message === 'import') {
      client.run()
    }
  })

  client.on('error', (msg) => {
    ws.send(pack({ message: 'error', ...msg }))
  })

  client.on('exit', (msg) => {
    ws.send(pack({ message: 'exit', entity: 'client', ...msg }))
  })
}

wss.on('connection', (ws) => {
  print('Connection Accepted!')
  handleConnection(ws)
})
