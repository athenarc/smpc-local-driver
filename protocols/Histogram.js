const path = require('path')
const { spawn } = require('child_process')

const Protocol = require('./Protocol')
const { pack } = require('../helpers')

const SCALE = process.env.SMPC_ENGINE
const SCRIPTS = path.resolve(__dirname, '../', 'scripts')
const PREPROCESS_CMD = `python3 ${SCRIPTS}/preprocessor.py`
const REQUEST_FOLDER = path.resolve(__dirname, '../', 'requests')
const DATASET_FOLDER = path.resolve(__dirname, '../', 'datasets')
const DATASET = process.env.DATASET || path.resolve(DATASET_FOLDER, 'dataset.csv')

class Histogram extends Protocol {
  constructor ({ ws, id }) {
    super({ ws, id })
  }

  handleClientError (msg) {
    this.ws.send(pack({ message: 'error', ...msg }))
  }

  handleClientExit (msg) {
    this.ws.send(pack({ message: 'exit', entity: 'client', ...msg }))
  }

  handleDataInfo (msg) {
    this.ws.send(pack({ message: 'data-info', ...msg }))
  }

  async preprocess () {
    if (!this.job) {
      this.handleClientError({ id: this.id, errors: ['Missing job description'] })
      this.client.terminate()
      return
    }

    const attr = this.job.attributes.map(a => `"${a.name}"`)
    const req = JSON.stringify(JSON.stringify(this.job)) // By appling two times stringify double quotes are escaped.
    const args = [`-c ${this.job.id}`, `-d ${path.resolve(__dirname, DATASET)}`, `-a ${attr.join(' ')}`, `-g ${this.job.algorithm}`, `-r ${req}`]
    this.preprocessCMD = spawn(PREPROCESS_CMD, args, { cwd: SCALE, shell: true, detached: true })

    this.preprocessCMD.stderr.on('data', (data) => { console.log(data.toString()) })
    this.preprocessCMD.on('exit', async (code) => {
      if (code !== 0) {
        this.handleClientError({ id: this.id, code, errors: this.errors })
        return
      }

      try {
        const datasetInfo = require(`${REQUEST_FOLDER}/${this.job.id}/${this.job.id}.json`)
        this.handleDataInfo({ id: this.id, datasetInfo: { ...datasetInfo } })
      } catch (e) {
        console.log(e)
        this.handleClientError({ id: this.id, errors: [e.message] })
      }
    })
  }

  handleClose ({ ws, code, reason }) {
    this.client.removeAllListeners()
    this.client.terminate()
  }

  handleError ({ ws, err }) {
    this.client.removeAllListeners()
    this.client.terminate()
  }

  handleMessage ({ ws, msg }) {
    if (msg.message === 'job-info') {
      this.client.setJob({ ...msg.job })
    }

    if (msg.message === 'data-info') {
      this.preprocess()
    }

    if (msg.message === 'import') {
      this.client.run()
    }

    if (msg.message === 'restart') {
      this.client.terminate()
    }
  }
}

module.exports = Histogram
