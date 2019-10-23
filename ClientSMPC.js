const { spawn } = require('child_process')
const EventEmitter = require('events')
const path = require('path')

const { includeError } = require('./helpers')

const SCALE = process.env.SMPC_ENGINE
const CLIENT_CMD = process.env.NODE_ENV === 'development' ? './fake_scale.sh' : `${SCALE}/Client-Api.x`
const REQUEST_FOLDER = path.resolve(__dirname, 'requests')

class Client extends EventEmitter {
  constructor (id) {
    super()
    this.client = null
    this.preprocessCMD = null
    this.id = id
    this.errors = []
    this.job = null
  }

  setJob (job) {
    this.job = job
  }

  run () {
    const cwd = process.env.NODE_ENV === 'development' ? __dirname : SCALE
    this.client = spawn(CLIENT_CMD, [this.id, `${REQUEST_FOLDER}/${this.job.id}/${this.job.id}.txt`], { cwd, shell: true, detached: true })

    this.client.stdout.on('data', (data) => {
      console.log(data.toString())
    })

    this.client.stderr.on('data', (data) => {
      data = data.toString().toLowerCase()
      if (includeError(data, ['what()', 'aborted'])) {
        this.errors.push(data)
        this.emit('error', { id: this.id, error: { message: data } })
      }
    })

    this.client.on('exit', (code) => {
      console.log(`Client exited with code ${code}`)
      this.emit('exit', { id: this.id, code, error: { message: this.errors.join(' ') } })
      this.terminate()
    })
  }

  terminate () {
    if (this.client) {
      this.client.removeAllListeners()
      this.client.stdin.pause()
      try {
        process.kill(-this.client.pid)
      } catch (e) {}
      this.client.kill()
      this.client = null
    }

    if (this.preprocessCMD) {
      this.preprocessCMD.removeAllListeners()
      this.preprocessCMD.stdin.pause()
      try {
        process.kill(-this.preprocessCMD.pid)
      } catch (e) {}
      this.preprocessCMD.kill()
      this.preprocessCMD = null
    }

    this.job = null
  }
}

module.exports = Client
