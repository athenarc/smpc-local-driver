const { spawn } = require('child_process')
const EventEmitter = require('events')
const path = require('path')
const fs = require('fs')

const { includeError } = require('./helpers')

const SCALE = process.env.SMPC_ENGINE
const SCRIPTS = path.resolve(__dirname, 'scripts')
const CLIENT_CMD = process.env.NODE_ENV === 'development' ? './fake_scale.sh' : `${SCALE}/Client-Api.x`
const PREPROCESS_CMD = `python3 ${SCRIPTS}/preprocessor.py`
const REQUEST_FOLDER = path.resolve(__dirname, 'requests')
const DATASET_FOLDER = path.resolve(__dirname, 'datasets')
const DATASET = process.env.DATASET || path.resolve(DATASET_FOLDER, 'dataset.csv')

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

  async readFirstLine (path, opts = { encoding: 'utf8', lineEnding: '\n' }) {
    // Code from: https://github.com/pensierinmusica/firstline/blob/master/index.js
    return new Promise((resolve, reject) => {
      const rs = fs.createReadStream(path, { encoding: opts.encoding })
      let acc = ''
      let pos = 0
      let index
      rs
        .on('data', chunk => {
          index = chunk.indexOf(opts.lineEnding)
          acc += chunk
          if (index === -1) {
            pos += chunk.length
          } else {
            pos += index
            rs.close()
          }
        })
        .on('close', () => resolve(acc.slice(acc.charCodeAt(0) === 0xFEFF ? 1 : 0, pos)))
        .on('error', err => reject(err))
    })
  }

  async preprocess () {
    if (!this.job) {
      this.emit('error', { id: this.id, errors: ['Missing job description'] })
      this.terminate()
      return
    }

    const attr = this.job.attributes.map(a => `"${a.name}"`)
    const req = JSON.stringify(JSON.stringify(this.job)) // By appling two times stringify double quotes are escaped.
    const args = [`-c ${this.job.id}`, `-d ${path.resolve(__dirname, DATASET)}`, `-a ${attr.join(' ')}`, `-g ${this.job.algorithm}`, `-r ${req}`]
    this.preprocessCMD = spawn(PREPROCESS_CMD, args, { cwd: SCALE, shell: true, detached: true })

    this.preprocessCMD.stderr.on('data', (data) => { console.log(data.toString()) })
    this.preprocessCMD.on('exit', async (code) => {
      if (code !== 0) {
        this.emit('error', { id: this.id, code, errors: this.errors })
        return
      }

      try {
        const datasetInfo = require(`${REQUEST_FOLDER}/${this.job.id}/${this.job.id}.json`)
        this.emit('data-info', { id: this.id, datasetInfo: { ...datasetInfo } })
      } catch (e) {
        console.log(e)
        this.emit('error', { id: this.id, errors: [e.message] })
      }
    })
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
        this.emit('error', { id: this.id, errors: this.errors })
      }
    })

    this.client.on('exit', (code) => {
      console.log(`Client exited with code ${code}`)
      this.emit('exit', { id: this.id, code, errors: this.errors })
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
