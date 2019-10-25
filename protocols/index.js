const HistogramProtocol = require('./Histogram')
const DockerImageProtocol = require('./DockerImage')

const protocolMapping = new Map()

/* All available protocols */
protocolMapping.set('histogram', HistogramProtocol)
protocolMapping.set('dockerImage', DockerImageProtocol)

module.exports = {
  protocolMapping
}
