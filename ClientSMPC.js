const { spawn } = require('child_process')
const EventEmitter = require('events')
const path = require('path')
const fs = require('fs')

const { includeError } = require('./helpers')

const SCALE = process.env.SMPC_ENGINE
const CLIENT_CMD = `${SCALE}/Client-Api.x`
const TOTAL_PLAYERS = 3
const DATASET_FOLDER = process.env.DATASET_FILE || path.resolve(__dirname, 'datasets')

class Client extends EventEmitter {
  constructor (id) {
    super()
    this.client = null
    this.id = id
    this.errors = []
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

  async dataSize () {
    const dataset = `${DATASET_FOLDER}/Client_data${this.id}.txt` // file name should change. In real enviroments each client is in different machine.
    const dataSize = await this.readFirstLine(dataset)
    this.emit('data-size', { id: this.id, dataSize })
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
      this.terminate()
    })
  }

  terminate () {
    if (this.client) {
      this.client.stdin.pause()
      this.client.kill()
      this.client = null
    }
  }
}

module.exports = Client
