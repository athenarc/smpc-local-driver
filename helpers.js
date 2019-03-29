const PREFIX = `Client WebSocket: ID: ${process.env.ID}: `

const print = (message) => {
  console.log(`${PREFIX}`, message)
}

const pack = (msg) => {
  return JSON.stringify(msg)
}

const unpack = (msg) => {
  return JSON.parse(msg)
}

module.exports = {
  print,
  pack,
  unpack
}
