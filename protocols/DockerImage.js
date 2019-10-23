const Docker = require('dockerode')
const Protocol = require('./Protocol')
const { pack, promisifyStream, getProgress } = require('../helpers')
const logger = require('../config/winston')

class DockerImage extends Protocol {
  constructor ({ ws }) {
    super({ ws })
    this.docker = new Docker({ socketPath: process.env.DOCKER_HOST || '/var/run/docker.sock' })
  }

  /* Abstract Methods */
  handleClose ({ ws, code, reason }) {}

  handleError ({ ws, err }) {}

  async pullImage (req) {
    const image = this.job.image

    try {
      const stream = await this.docker.createImage({ fromImage: image })
      await promisifyStream(stream, (data) => console.log(getProgress(['id', 'status', 'progress'], data.toString())))
      this.ws.send(pack({ message: 'image-imported' }))
    } catch (err) {
      logger.error(err)
      this.ws.send(pack({ message: 'error', error: { message: 'Error creating docker image' } }))
    }
  }

  handleMessage ({ ws, msg }) {
    if (msg.message === 'job-info') {
      this.job = { ...msg.job }
    }

    if (msg.message === 'import-image') {
      console.log(this.id)
      this.pullImage()
    }
  }

  handleClientError (msg) {}

  handleClientExit (msg) {}
}

module.exports = DockerImage
