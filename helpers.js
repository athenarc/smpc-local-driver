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

const includeError = (str, err) => {
  return err.some((el) => str.includes(el))
}

const promisifyStream = (stream, handler) => new Promise((resolve, reject) => {
  stream.on('data', d => handler(d))
  stream.on('end', resolve)
  stream.on('error', reject)
})

const getProgress = (args, data) => {
  try {
    let res = JSON.parse(data.toString())

    /* Filter undefined variables */
    res = args.map(arg => res[arg]).filter(item => item)
    return res.join(': ')
  } catch (err) {
    return ''
  }
}

module.exports = {
  print,
  pack,
  unpack,
  includeError,
  promisifyStream,
  getProgress
}
