const winston = require('winston')
const express = require('express')

const router = express.Router()

router.get('/import/numeric', async (req, res, next) => {
  try {
    res.status(200).json({})
  } catch (err) {
    next(err)
  }
})

router.get('/import/categorical', async (req, res, next) => {
  try {
    res.status(200).json({})
  } catch (err) {
    next(err)
  }
})

module.exports = router
