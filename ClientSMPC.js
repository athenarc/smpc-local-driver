const { spawn } = require('child_process')
const EventEmitter = require('events')

const SCALE = process.env.SMPC_ENGINE
const CLIENT_CMD = `${SCALE}/Client-Api.x`
const TOTAL_PLAYERS = 3

class Client extends EventEmitter {
  constructor (id) {
    super()
    this.client = null
    this.id = id
  }

  run () {
    this.client = spawn(CLIENT_CMD, [this.id, TOTAL_PLAYERS], { cwd: SCALE, shell: true })

    this.client.stdout.on('data', (data) => {
      console.log(data.toString())
    })

    this.client.stderr.on('data', (data) => {
      console.log(data.toString())
    })

    this.client.on('exit', (code) => {
      console.log(`Client exited with code ${code}`)
      this.emit('exit', { id: this.id })
      this.client = null
    })
  }
}

module.exports = Client
