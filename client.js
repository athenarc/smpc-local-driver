require('dotenv').config()

const _ = require('lodash')
const https = require('https')
const fs = require('fs')
const WebSocket = require('ws')
const Client = require('./ClientSMPC')
const { print, pack, unpack } = require('./helpers')

const PORT = process.env.PORT || 3004

if (_.isEmpty(process.env.ROOT_CA)) {
  throw new Error('HTTPS root CA path must be defined!')
}

if (_.isEmpty(process.env.PEM_KEY)) {
  throw new Error('HTTPS key path must be defined!')
}

if (_.isEmpty(process.env.SMPC_ENGINE)) {
  throw new Error('SMPC Engine absolute path not defined!')
}

if (!process.env.ID) {
  throw new Error('Client ID not defined!')
}

const server = https.createServer({
  ca: fs.readFileSync(process.env.ROOT_CA, { encoding: 'utf-8' }),
  cert: fs.readFileSync(process.env.PEM_CERT),
  key: fs.readFileSync(process.env.PEM_KEY),
  requestCert: true,
  rejectUnauthorized: true,
  port: PORT,
  clientTracking: true
})

const wss = new WebSocket.Server({ server })

console.log(`Client ${process.env.ID} started on port ${PORT}.`)

const handleConnection = (ws) => {
  const client = new Client(process.env.ID)

  ws.on('close', (data) => {
    console.log('Connection Closed: ', data)
    client.removeAllListeners()
    client.terminate()
  })

  ws.on('error', (err) => {
    console.log(err)
    client.removeAllListeners()
    client.terminate()
  })

  ws.on('message', (data) => {
    print(`Message: ${data}`)
    data = unpack(data)

    if (data.message === 'job-info') {
      client.setJob({ ...data.job })
    }

    if (data.message === 'data-info') {
      client.preprocess()
    }

    if (data.message === 'import') {
      client.run()
    }

    if (data.message === 'restart') {
      client.terminate()
    }
  })

  client.on('data-info', (msg) => {
    ws.send(pack({ message: 'data-info', ...msg }))
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

server.listen(process.env.PORT)
