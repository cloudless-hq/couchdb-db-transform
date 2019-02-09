// -------------------------------------------------------------------------------
// Copyright IBM Corp. 2016
//
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// -------------------------------------------------------------------------------

const async = require('async')

const debug = require('debug')('cloudless' + ':replicate')
const debugData = require('debug')('cloudless' + ':data')
const debugPerf = require('debug')('cloudless' + ':performance')

const mutil = require('./util.js')
const cloudant = require('@cloudant/cloudant')
const transform = require('./transform')
const filter = require('./filter')
const EventSource = require('eventsource')

const concurrency = 3
const changesPerBatch = 70
const maxQueuedBatches = 20
const checkQueueSizeInterval = 5000
const inactivityCheckInterval = 7000
const maxInactivityCheckIntervalDelayFactor = 4 // inactivityCheckInterval * 2^X = (maximum interval)

/*
 * Replication function. Listens to the change feed of the database identified by <source>,
 * and selectively applies those changes to the database identified by <target>.
 *
 * @param {Object} source - credentials for the source database
 * @param {String} source.url - the URL of the source database
 * @param {String} source.dbname - the name of the source database
 * @param {Object} target - credentials for the target database
 * @param {String} target.url - the URL of the target database
 * @param {String} target.dbname - the name of the target database
 * @param {Boolean} restart - if true (false is default), process all document changes in the source database; if false process changes that were not yet processed
*/
const state = {
  source: {},
  target: {},
  seqsSeen: {},
  // set the default (true: process all document changes,
  // false: process document changes that were not yet processed)
  restart: false,
  // service status information
  stats: {
    initialized: false, // service status
    source: {
      // databaseName: null, // source database name
      // last_change_received: null,// timestamp for last change notification that was received from the source database
      // update_seq: null // most current seq number
    },
    target: {
      // databaseName: null, // target database name
      // last_change_applied: null, // timestamp for last write operation in the target database
      // lastAppliedUpdateSeq: null, // CouchDB update_seq number for the last change that was written to the target database
      // copied: 0, // document copy successes
      // failed: 0, // document copy failures
    },
    filter: {
      server: null, // server-side filter info
      client: null // client-side filter info
    },
    transformer: null // transformer info
  },
  sourceCloudant: null
}

module.exports.init = function ({ source, target, restart, initCallback }) {
  // connect to source system
  state.source = source
  state.target = target
  state.restart = restart || false

  state.sourceCloudant = cloudant({
    url: state.source.url,
    plugins: [],
    requestDefaults: {
      headers: {
        'Content-Type': 'application/json',
        'cf-access-token': process.env.cfAccessToken
      }
    }
  })

  // verify that the source database exists
  state.sourceCloudant.db.get(state.source.dbname, function (err, body) {
    if (err) {
      return initCallback('The source database "' + state.source.dbname + '" cannot be accessed: ' + err)
    }

    debug('Source database information: ' + JSON.stringify(body))

    state.stats.source = {
      databaseName: state.source.dbname
    }

    const sourceDb = state.sourceCloudant.db.use(state.source.dbname)

    // connect to target system
    const targetCloudant = cloudant({
      url: state.target.url,
      plugins: [],
      requestDefaults: {
        headers: {
          'Content-Type': 'application/json',
          'cf-access-token': process.env.cfAccessToken
        }
      }
    })

    // verify that the target database exists
    targetCloudant.db.get(state.target.dbname, function (err, body) {
      if (err) {
        return initCallback('The target database "' + state.target.dbname + '" cannot be accessed: ' + err)
      }

      debug('Target database information: ' + JSON.stringify(body))

      state.stats.target = {
        databaseName: state.target.dbname,
        lastAppliedUpdateSeq: 0,
        copied: 0,
        failed: 0,
        conflicts: 0
      }

      const targetDb = targetCloudant.db.use(state.target.dbname)

      // bulk insert documents into the target database
      let q = async.queue((batch, callback) => {
        debug('Saving new batch of documents. Batch size is ' + batch.changes.length)

        let seq = []
        let docs = []
        batch.changes.forEach(change => {
          seq.push(change.seq)
          docs.push(change.doc)
        })

        targetDb.bulk({ docs }, (err, data) => {
          if (err) {
            // an error was returned assume that none of the documents was successfully stored
            state.stats.target.failed = state.stats.target.failed + batch.changes.length
            return callback(
              'Error saving documents in target database "' +
              state.target.dbname + '": ' + err
            )
          } else {
            // keep track of when the last change was written to the target database
            state.stats.target.last_change_applied = Date()

            let lastSuccessSeqInBatch = null
            let errors = []

            data.forEach((result, index) => {
              if (result.id && result.rev) {
                state.stats.target.copied++
                lastSuccessSeqInBatch = seq[index]
              } else {
                if (result.error === 'conflict') {
                  state.stats.target.conflicts++
                  lastSuccessSeqInBatch = seq[index]
                } else {
                  errors.push(result)
                  state.stats.target.failed++
                }
              }
            })

            if (errors.length > 0) {
              return callback(
                'Error saving documents in target database "' +
                state.target.dbname +
                '": ' + JSON.stringify(errors)
              )
            }

            state.stats.target.lastAppliedUpdateSeq = lastSuccessSeqInBatch
            callback()
          }
        })
      }, concurrency)

      // load transformation routine, if configured
      transform.getTransformer((err, transformer) => {
        if (err) {
          return initCallback(err)
        }

        state.stats.transformer = {
          name: transformer.getName(),
          definition: transformer.getRoutineDefinition()
        }

        // load filters, if configured
        filter.getFilter(sourceDb, (err, filter) => {
          if (err) {
            return initCallback(err)
          }

          state.stats.filter.server = {
            name: filter.getServerFilterName(),
            definition: filter.getServerFilterDefinition()
          }
          state.stats.filter.client = {
            name: filter.getClientFilterName(),
            definition: filter.getClientFilterDefinition(),
            filtered: 0
          }

          console.log('Replicator ready. Starting to listen for changes in database "' +
                state.source.dbname + '". Changes will be applied to database "' +
                state.target.dbname + '".')

          debug(JSON.stringify({
            source: mutil.getCredentialsWithoutPassword(state.source),
            target: mutil.getCredentialsWithoutPassword(state.target)
          }))

          let changes = []
          let i = 0
          const processChange = e => {
            const change = JSON.parse(e.data)
            if (filter.applyClientFilter(change)) {
              // status monitoring: keep track of when the last change notification was received
              state.stats.source.last_change_received = Date()

              // if (state.seqsSeen[change.seq]) {
              //   console.error('################# FEED LOOP DETECTED!!!!! ##################')
              // }

              state.stats.target.lastReceivedUpdateSeq = change.seq

              // state.seqsSeen[change.seq] = true
              i++

              if (i % 200 === 0) {
                debugData('________________________________________')
                debugData('change ' + i)
                debugData('seq ' + change.seq)
                debugData('queue length ' + q.length())
                debugData('________________________________________')
              }

              // updates are currently not supported remove _rev from document
              // note that this might cause document update conflicts
              delete change.doc._rev

              // invoke transformation routine
              transformer.transform(change.doc,
                (err, transformedDoc) => {
                  if (!transformedDoc) {
                    state.stats.filter.client.filtered++
                    debug('Ignored change in document ' + change.doc._id + '.')
                    return
                  }
                  if (err) {
                    return initCallback(err)
                  }

                  // add change to batch
                  changes.push({
                    seq: change.seq,
                    doc: transformedDoc
                  })

                  if (changes.length >= changesPerBatch) {
                    q.push({
                      changes: changes.splice(0, changesPerBatch)},
                      function (err) {
                        if (err) {
                          console.error('Worker returned an error: ' + err)
                        }
                        console.log('batch save completed.')
                        // console.log('Status summary: ' + JSON.stringify(getStatus()))
                      }
                    )
                  }
                })
            } else {
              state.stats.filter.client.filtered++
              debug('Ignored change in document ' + change.doc._id + '.')
            }
          }

          // Throttle change feed to limit memory consumption check queue size every
          // <checkQueueSizeInterval> ms and pause if more than <maxQueuedBatches>
          // are waiting to be processed
          let feedPaused = false
          let feed
          function startFeed (updateStartSequence, cbb) {
            console.log('############ (re)starting feed since ' + updateStartSequence + ' ######################')
            let feedUrl = state.source.url + '/' + state.source.dbname + '/_changes?feed=eventsource&seq_interval=1&style=main_only&conflicts=false&include_docs=true&since=' + updateStartSequence
            if (filter.hasServerFilter()) {
              feedUrl = feedUrl + '&filter=' + filter.getServerFilterName()
            }
            feed = new EventSource(feedUrl, {
              headers: { 'cf-access-token': process.env.cfAccessToken }
            })
            feed.addEventListener('message', cbb)
          }

          startFeed(state.stats.target.lastAppliedUpdateSeq || 0, processChange)

          setInterval(() => {
            if (q.length() > maxQueuedBatches) {
              if (!feedPaused) {
                feed.close()
                feed = null
                feedPaused = true
                debugPerf('Pausing change feed to reduce memory consumption. Queue size: ' + q.length())
                // debugPerf('Memory utilization: ' + JSON.stringify(process.memoryUsage()))
              }
            } else {
              if (feedPaused && q.length() < maxQueuedBatches / 2) {
                startFeed(state.stats.target.lastAppliedUpdateSeq, processChange)
                feedPaused = false
                debugPerf('Resuming change feed. Queue size: ' + q.length())
              }
            }
          }, checkQueueSizeInterval)

          /*
           * Monitor replication status every <X> ms. If no change activity was reported by the source
           * flush the document buffer. X is calculated as follows:
           *  <inactivityCheckInterval> * (Math.pow(2,<inactivityCheckIntervalDelayFactor>))
           *  <inactivityCheckIntervalDelayFactor> is increased if the system is found idle
           */
          let inactivityCheckIntervalDelayFactor = 0
          let flushTimer = null

          let flush = function () {
            if (changes.length === 0) {
              if (q.idle()) {
                if (inactivityCheckIntervalDelayFactor <= maxInactivityCheckIntervalDelayFactor) {
                  inactivityCheckIntervalDelayFactor++
                  clearInterval(flushTimer)
                  flushTimer = setInterval(
                    flush,
                    inactivityCheckInterval * (Math.pow(2, inactivityCheckIntervalDelayFactor))
                  )
                }
              }
              console.log(Date() + ' Document buffer is empty. Next attempt to flush buffer will be made in ' + (inactivityCheckInterval * (Math.pow(2, inactivityCheckIntervalDelayFactor)) / 1000) + ' seconds.')
            } else {
              if (q.idle()) {
                // there's at least one document in the buffer that has not been sent to the target
                // write the buffer content to the target
                console.log('Flushing buffer containing ' + changes.length + ' documents.')
                q.push({
                  changes: changes.splice(0, changesPerBatch)
                },
                function (err) {
                  if (err) {
                    console.error('Worker returned an error: ' + err)
                  }
                  console.log('Status summary: ' + JSON.stringify(getStatus()))
                })
              }
              inactivityCheckIntervalDelayFactor = 0
              clearInterval(flushTimer)
              flushTimer = setInterval(
                flush,
                inactivityCheckInterval * (Math.pow(2, inactivityCheckIntervalDelayFactor))
              )
            }
          }

          // interval timer: flush partially filled document buffer if the change feed is idle
          flushTimer = setInterval(
            flush,
            inactivityCheckInterval * (Math.pow(2, inactivityCheckIntervalDelayFactor))
          )

          state.stats.initialized = true

          // signal to the caller that initialization has completed
          return initCallback()
        })
      })
    })
  })
}

/*
 * Returns detailed system status information
 * @return {Object} status
 * @return {Numeric} status.copied - documents copied
 * @return {Numeric} status.failed - document copy failures
 * @return {Numeric} status.filtered - documents that were not copied because the filter condition was met
 * @return {String} status.last_change_received - timestamp for last change notification that was received from the source database
 * @return {String} status.last_change_applied - timestamp for last write operation in the target database
 * @return {String} status.last_update_seq - CouchDB update_seq number for the last change that was written to the target database
 * @return {Object} status.filter - filter information
 * @return {Object} status.filter.server - server-side filter information
 * @return {String} status.filter.server.name - if defined, <design_doc/filter> containing the filter routine
 * @return {String} status.filter.server.definition - the routine text of status.filter.server.name
 * @return {Object} status.filter.client - client-side filter information
 * @return {String} status.filter.client.name if - defined, <design_doc/filter> containing the filter routine
 * @return {String} status.filter.client.definition - the routine text of status.filter.client.name
 * @return {Object} status.transformer - transformer information
 * @return {String} status.filter.transformer.name - if defined, the file name containing the transformation routine
 * @return {String} status.filter.transformer.definition - the routine text of status.filter.transformer.name
 */

module.exports.getDetailedStatus = function (callback) {
  let status = {
    status_date: Date(),
    service_status: {
      source: {
        databaseName: state.stats.source.databaseName,
        last_change_received: state.stats.source.last_change_received
      },
      target: state.stats.target,
      filter: state.stats.filter,
      transformer: state.stats.transformer
    }
  }

  if (state.sourceCloudant) {
    debug('Getting metadata for source database ' + state.source.dbname)
    state.sourceCloudant.db.get(state.source.dbname, function (err, body) {
      if (err) {
        console.error('The source database "' + state.source.dbname + '" cannot be accessed: ' + err)
        status.service_status.source.update_seq = '<Not available>'
      } else {
        status.service_status.source.update_seq = body.update_seq
      }
      return callback(null, status)
    })
  } else {
    return callback(null, status)
  }
}

/*
 * Returns system status information
 * @return {Object} status
 * @return {Object} status.source
 * @return {String} status.source.databaseName - source database name
 * @return {String} status.source.last_change_received - timestamp for last change notification that was received from the source database
 * @return {Object} status.target
 * @return {String} status.target.databaseName - target database name
 * @return {String} status.target.lastAppliedUpdateSeq - CouchDB update_seq number for the last change that was written to the target database
 * @return {Numeric} status.target.copied - documents copied
 * @return {Numeric} status.target.failed - document copy failures
 * @return {String} status.target.last_change_applied - timestamp for last write operation in the target database
 * @return {Numeric} status.filtered - documents that were not copied because the filter condition was met
 */
function getStatus () {
  if (state.stats.filter.client) {
    return {
      source: state.stats.source,
      target: state.stats.target,
      filtered: state.stats.filter.client.filtered
    }
  }

  return {
    source: state.stats.source,
    target: state.stats.target
  }
}

module.exports.getStatus = getStatus
