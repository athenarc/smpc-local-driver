require('dotenv').config()

const _ = require('lodash')
const WebSocket = require('ws')
const Client = require('./ClientSMPC')
const { print, pack, unpack } = require('./helpers')

const PORT = process.env.PORT || 3004

if (_.isEmpty(process.env.SMPC_ENGINE)) {
  throw new Error('SMPC Engine absolute path not defined!')
}

if (!process.env.ID) {
  throw new Error('Client ID not defined!')
}

const wss = new WebSocket.Server({ port: PORT, clientTracking: true })

console.log(`Client ${process.env.ID} started on port ${PORT}.`)

const handleConnection = (ws) => {
  const client = new Client(process.env.ID)

  ws.on('close', (data) => {
    console.log('Connection Closed: ', data)
    client.terminate()
  })

  ws.on('error', (err) => {
    console.log(err)
    client.terminate()
  })

  ws.on('message', (data) => {
    print(`Message: ${data}`)
    data = unpack(data)

    if (data.message === 'data-size') {
      client.dataSize()
    }

    if (data.message === 'import') {
      client.run()
    }

    if (data.message === 'restart') {
      client.terminate()
    }
  })

  client.on('data-size', (msg) => {
    ws.send(pack({ message: 'data-size', ...msg }))
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
