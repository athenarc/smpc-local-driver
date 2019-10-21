require('dotenv').config()

const _ = require('lodash')
const https = require('https')
const fs = require('fs')
const WebSocket = require('ws')
const ProtocolHandler = require('./protocols/ProtocolHandler')
const { print } = require('./helpers')

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

wss.on('connection', (ws) => {
  print('Connection Accepted!')
  const client = new ProtocolHandler({ ws, id: process.env.ID })
})

server.listen(process.env.PORT)
