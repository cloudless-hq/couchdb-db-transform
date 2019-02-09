// -------------------------------------------------------------------------------
// Copyright IBM Corp. 2016
//
// Licensed under the Apache License, Version 2.0 (the 'License')
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

const express = require('express')
const bodyParser = require('body-parser')

// to enable debugging, set environment variable DEBUG to slack-about-service or *
const debug = require('debug')('cloudless')

const { init, getDetailedStatus } = require('./lib/replicate.js')
const mutil = require('./lib/util.js')

/*
 *
 * Environment variable dependencies:
 *  - SOURCE_COUCH_DB_URL: https://$USERNAME:$PASSWORD@$REMOTE_USERNAME.cloudant.com/$SOURCE_DATABASE_NAME
 *  - TARGET_COUCH_DB_URL: https://$USERNAME:$PASSWORD@$REMOTE_USERNAME.cloudant.com/$TARGET_DATABASE_NAME
 *  - RESTART (optional, default is false): if true, the change feed will process all document changes since the database was created otherwise
 *      only new document changes will be processed
 *  - TRANSFORM_FUNCTION (optional, default no transformation): file containing the Javascript routine to be used to transform documents
 *  - SERVER_FILTER (optional, no default): name of an existing filter in the source database, expressed as "$DESIGN_DOC_NAME/$FILTER_NAME"
 *  - CLIENT_FILTER (optional, no default): name of a file in the application directory containing a filter function: $PATH_TO/$FILTER_FUNCTION_FILE_NAME
 *  - HIDE_CONSOLE (optional, default false): disables all API endpoints
 *  - DEBUG (optional): if set to * or $APP_PREFIX:$MODULE, debug information is added to the log
 */

debug('cloudless' + ': debug is enabled.')

/*
 * Verify that the application was properly configured
*/

init({
  source: mutil.splitUrl(process.env.SOURCE_COUCH_DB_URL),
  target: mutil.splitUrl(process.env.TARGET_COUCH_DB_URL),
  restart: mutil.isTrue(process.env.RESTART),

  initCallback: function (err) {
    if (err) {
      console.error('Error. Service initialization failed: ' + err)
      console.error('Check the log file for additional information.')
      console.error('The service is stopping.')
      process.exit(1)
    }

    var app = express()
    app.use(bodyParser.urlencoded({extended: false}))

    if (!process.env.HIDE_CONSOLE) {
      // replication status endpoint
      app.get('/status',
        function (req, res) {
          getDetailedStatus((err, status) => {
            if (err) {
              return res.status(500).json(err)
            }

            return res.status(200).json(status)
          })
        })
    }

    // start server on the specified port and binding host
    app.listen(8083, '0.0.0.0', function () {
      console.log('Server starting ')
      if (!process.env.HIDE_CONSOLE) {
        console.log('Status information is made available at /status')
        console.log('To disable status output, set environment variable HIDE_CONSOLE to true and restart the application.')
      }
    })
  }
})
