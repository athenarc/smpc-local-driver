const { spawn } = require('child_process')
const EventEmitter = require('events')

const { includeError } = require('./helpers')

const SCALE = process.env.SMPC_ENGINE
const CLIENT_CMD = `${SCALE}/Client-Api.x`
const TOTAL_PLAYERS = 3

class Client extends EventEmitter {
  constructor (id) {
    super()
    this.client = null
    this.id = id
    this.errors = []
  }

  run () {
    this.client = spawn(CLIENT_CMD, [this.id, TOTAL_PLAYERS], { cwd: SCALE, shell: true })

    this.client.stdout.on('data', (data) => {
      console.log(data.toString())
    })

    this.client.stderr.on('data', (data) => {
      data = data.toString().toLowerCase()
      if (includeError(data, ['what()', 'aborted'])) {
        this.errors.push(data)
        this.emit('error', { id: this.id, errors: this.errors })
      }
    })

    this.client.on('exit', (code) => {
      console.log(`Client exited with code ${code}`)
      this.emit('exit', { id: this.id, code, errors: this.errors })
      this.client.stdin.pause()
      this.client.kill()
      this.client = null
    })
  }
}

module.exports = Client
