'use strict'

const PACKAGE = require('./package.json')
const VERSION = PACKAGE.version

const PAKO = require('pako')

const STAGE = process.env.STAGE || 'test'
const DYNAMO_NS = `pg_${STAGE}-nsTable`
// Configure Kubernetes-Client with inCluster service account credentials
const Client = require('kubernetes-client').Client
const Request = require('kubernetes-client/backends/request')
let KUBERNETES

const AWS = require('aws-sdk')
AWS.config.update({region:'us-east-1', correctClockSkew: true})
const SQS = new AWS.SQS()
const DYNAMO = new AWS.DynamoDB.DocumentClient()

let PARAMS = {}
let LOGSTREAMS = {}
let IOT

console.log(`Worker Manager Version: ${VERSION} :: Stage: ${STAGE}`)
console.log(`ENV: ${JSON.stringify(process.env)}`)

async function configAWS() {
  IOT = new AWS.IotData({endpoint: `${PARAMS.IOT_ENDPOINT_HOST}`, correctClockSkew: true})
}

// Logger intercept for easy logging in the future.
const LOGGER = {
  info: (msg, userId = 'unset') => {
    if (userId !== 'unset') {
      console.log(`WORKERS: [${userId}] ${msg}`)
      let notificationTopic = `${STAGE}/frontend/${userId}`
      mqttSend(notificationTopic, {notification: {type: 'success', msg: msg}})
    } else {
      console.log(`WORKERS: ${msg}`)
    }
  },
  error: (msg, userId = 'unset') => {
    if (userId !== 'unset') {
      console.error(`WORKERS: [${userId}] ${msg}`)
      let notificationTopic = `${STAGE}/frontend/${userId}`
      mqttSend(notificationTopic, {notification: {type: 'error', msg: msg}})
    } else {
      console.error(`WORKERS: ${msg}`)
    }
  },
  debug: (msg, userId = 'unset') => {
    if (userId !== 'unset') {
      console.log(`WORKERS: [${userId}] ${msg}`)
    } else {
      console.log(`WORKERS: ${msg}`)
    }
  }
}

// Create Service
async function createService(cmd, fullMsg) {
  let data = fullMsg[cmd]
  try {
    let params = urlEncode(btoa(JSON.stringify({
      userId: fullMsg.userId,
      id: fullMsg[cmd].id,
      profileNum: fullMsg[cmd].profileNum,
      password: fullMsg[cmd].isyPassword
    })))
    let devMode = data.development || false
    let image
    if (PARAMS.hasOwnProperty('NS_IMAGE_PREFIX')) {
      image = PARAMS.NS_IMAGE_PREFIX
    } else {
      image = `einstein42/pgc_nodeserver:`
    }
    if (STAGE === 'test') { image += `beta_`}
    data.language.toLowerCase().includes('python') ? image += 'python' : image += 'node'
    if (image === `einstein42/pgc_nodeserver:`) {
      LOGGER.error(`createService: Bad Image: ${image}`, fullMsg.userId)
      return false
    }
    let PGURL=`${PARAMS.NS_DATA_URL}${params}`
    let name = `${fullMsg[cmd].name.toLowerCase()}-${fullMsg[cmd].id.replace(/:/g, '')}-${fullMsg[cmd].profileNum}`
    const deploymentManifest = {
      apiVersion: 'apps/v1',
      kind: 'Deployment',
      metadata: {
        name: name,
        namespace: 'nodeservers',
        labels: {
          user: `${fullMsg.userId}`,
          isy: `${fullMsg[cmd].id.replace(/:/g, '')}`,
          profileNum: `${fullMsg[cmd].profileNum}`
        }
      },
      spec: {
        replicas: devMode ? 0 : 1,
        selector: {
          matchLabels: {
            nodeserver: name
          }
        },
        template: {
          metadata: {
            labels: {
              nodeserver: name,
              user: `${fullMsg.userId}`,
              isy: `${fullMsg[cmd].id.replace(/:/g, '')}`,
              profileNum: `${fullMsg[cmd].profileNum}`
            }
          },
          spec: {
            containers: [
              {
                name: name,
                env: [
                  {
                    name: 'PGURL',
                    value: PGURL
                  }
                ],
                image: image,
                imagePullPolicy: 'Always'
              }
            ],
            terminationGracePeriodSeconds: 10
          }
        }
      }
    }
    const deployment = await KUBERNETES.apis.apps.v1.namespaces('nodeservers').deployments.post({ body: deploymentManifest })
    if (deployment.statusCode != 201) {
      LOGGER.error(`createService: Failed to create deployment. ${JSON.stringify(deployment)}`, fullMsg.userId)
      return false
    }
    let nodePort = 0
    let httpsIngress = false
    if (data.ingressRequired || data.tcpIngress) {
      LOGGER.debug(`createService: ingressRequired set creating Service`, fullMsg.userId)
      const serviceManifest = {
        kind: 'Service',
        apiVersion: 'v1',
        metadata: {
            name: `${name}-in`,
            namespace: 'nodeservers',
            labels: {
              user: `${fullMsg.userId}`,
              isy: `${fullMsg[cmd].id.replace(/:/g, '')}`,
              profileNum: `${fullMsg[cmd].profileNum}`
            }
        },
        spec: {
            type: 'NodePort',
            selector: {
                nodeserver: name
            },
            ports: [
                {
                    port: 3000,
                    name: 'tcpingress'
                }
            ]
        }
      }
      const nodeport = await KUBERNETES.api.v1.namespaces('nodeservers').services.post({ body: serviceManifest })
      if (nodeport.statusCode != 201) {
        LOGGER.error(`createService: Failed to create NodePort Service: ${JSON.stringify(nodeport)}`, fullMsg.userId)
      } else {
        nodePort = nodeport.body.spec.ports[0].nodePort
        LOGGER.debug(`createService: NodePort Service created: ${nodePort}`, fullMsg.userId)
      }
    }
    if (data.httpsIngress) {
      LOGGER.debug(`createService: httpsIngress set creating Service`, fullMsg.userId)
      const serviceManifest = {
        kind: 'Service',
        apiVersion: 'v1',
        metadata: {
            name: name,
            namespace: 'nodeservers',
            labels: {
              user: `${fullMsg.userId}`,
              isy: `${fullMsg[cmd].id.replace(/:/g, '')}`,
              profileNum: `${fullMsg[cmd].profileNum}`
            }
        },
        spec: {
            type: 'NodePort',
            selector: {
                nodeserver: name
            },
            ports: [
                {
                    port: 443,
                    targetPort: 3000,
                    name: 'httpsingress'
                }
            ]
        }
      }
      let inPort = 0
      const nodeportin = await KUBERNETES.api.v1.namespaces('nodeservers').services.post({ body: serviceManifest })
      if (nodeportin.statusCode != 201) {
        LOGGER.error(`createService: Failed to create Ingress NodePort Service: ${JSON.stringify(nodeportin)}`, fullMsg.userId)
      } else {
        inPort = nodeportin.body.spec.ports[0].nodePort
        LOGGER.debug(`createService: Ingress NodePort Service created: ${inPort}`, fullMsg.userId)
      }
      if (inPort !== 0) {
        LOGGER.debug(`createService: httpsIngress requested creating Ingress`, fullMsg.userId)
        const headers = {'content-type': 'application/json-patch+json'}
        const patchManifest = [{
          op: 'add',
          path: '/spec/rules/0/http/paths/-',
          value: {
            backend: {
              serviceName: name,
              servicePort: 443
            },
            path: `/ns/${name}/*`
          }
        }]
        const patchIngress = await KUBERNETES.apis.extensions.v1beta1.namespaces('nodeservers').ingresses('ns-ingress').patch({ headers: headers, body: patchManifest})
        if (patchIngress.statusCode != 200) {
          LOGGER.error(`createService: Failed to create Ingress: ${JSON.stringify(patchIngress)}`, fullMsg.userId)
        } else {
          httpsIngress = true
          LOGGER.debug(`createService: HTTPS Ingress created: ${name}`, fullMsg.userId)
        }
      } else {
        LOGGER.error(`createService: Failed to create Ingress NodePort skipping Ingress.`, fullMsg.userId)
      }
    }
    return {deployment: deployment.body, nodePort: nodePort, pgUrl: PGURL, httpsIngress}
  } catch (err) {
    LOGGER.error(`createService: ${err.stack}`, fullMsg.userId)
    return false
  }
}

async function removeDeployment(cmd, fullMsg, worker) {
  try {
    return await KUBERNETES.apis.apps.v1.namespaces('nodeservers').deployments(worker).delete()
  } catch (err) {
    LOGGER.error(`removeDeployment: ${err.stack}`, fullMsg.userId)
  }
}

async function removeService(cmd, fullMsg, worker) {
  try {
    return await KUBERNETES.api.v1.namespaces('nodeservers').services(worker).delete()
  } catch (err) {
    LOGGER.error(`removeService: ${err.stack}`, fullMsg.userId)
  }
}

async function removeIngress(cmd, fullMsg, worker) {
  try {
    const nsIngress = await KUBERNETES.apis.extensions.v1beta1.namespaces('nodeservers').ingresses('ns-ingress').get()
    const nsIndex = nsIngress.body.spec.rules[0].http.paths.findIndex(x => x.path.includes(worker))
    if (nsIndex > -1) {
      const headers = { 'content-type': 'application/json-patch+json' }
      const patchRemove = await KUBERNETES.apis.extensions.v1beta1.namespaces('nodeservers').ingresses('ns-ingress').patch(
        {
          headers: headers,
          body: [{
            op: 'remove',
            path: `/spec/rules/0/http/paths/${nsIndex}`
          }]
        })
      if (patchRemove.statusCode !== 200) {
        LOGGER.error(`removeIngress: failed to remove ${worker} :: ${JSON.stringify(patchRemove)}`, fullMsg.userId)
      }
    }
  } catch (err) {
    LOGGER.error(`removeIngress: ${err.stack}`, fullMsg.userId)
  }
}

// Get NodeServer from DB
async function getDbNodeServer(cmd, fullMsg) {
  let ns = fullMsg[cmd]
  let params = {
    TableName: DYNAMO_NS,
    KeyConditionExpression: "id = :id and profileNum = :profileNum",
    ExpressionAttributeValues: {
      ":id": ns.id,
      ":profileNum": `${ns.profileNum}`
    },
    ReturnValues: 'ALL'
  }
  try {
    let data = await DYNAMO.query(params).promise()
    if (data.hasOwnProperty('Items')) {
      return data.Items[0]
    } else {
      return false
    }
  } catch (err) {
    LOGGER.error(`getDbNodeServer: ${err.stack}`, fullMsg.userId)
  }
}

// Delete NodeServer from DB
async function deleteDbNodeServer(cmd, fullMsg) {
  let data = fullMsg[cmd]
  let params = {
    TableName: DYNAMO_NS,
    Key: {
      "id": data.id,
      "profileNum": `${data.profileNum}`
    },
    ReturnValues: 'ALL_OLD'
  }
  try {
    let data = await DYNAMO.delete(params).promise()
    if (data.hasOwnProperty('Attributes')) {
      return data.Attributes
    } else {
      return false
    }
  } catch (err) {
    LOGGER.error(`deleteDbNodeServer: ${err.stack}`, fullMsg.userId)
  }
}


async function updateClientNodeServers(id, fullMsg) {
  await mqttSend(`${STAGE}/isy`, {
    userId: fullMsg.userId,
    id: id,
    getNodeServers: {},
    topic: `${STAGE}/frontend/${fullMsg.userId}`
  }, fullMsg)

}

// Create NS in nsTable
async function createNS(cmd, fullMsg, worker) {
  let data = fullMsg[cmd]
  let params = {
    TableName: DYNAMO_NS,
    Key: {
      "id": data.id,
      "profileNum": `${data.profileNum}`
    },
    UpdateExpression: `
      SET #name = :name,
      nodes = :nodes,
      #type = :type,
      timeAdded = :timeAdded,
      isyUsername = :isyUsername,
      isyPassword = :isyPassword,
      #connected = :isConnected,
      worker = :worker,
      netInfo = :netInfo,
      #url = :url,
      #lang = :lang,
      #version = :version,
      timeStarted = :timeStarted,
      userId = :userId,
      isyVersion = :isyVersion,
      shortPoll = :shortPoll,
      longPoll = :longPoll,
      customParams = :customParams,
      customData = :customData,
      notices = :notices,
      logBucket = :logBucket,
      oauth = :oauth,
      firstRun = :firstRun,
      pgUrl = :pgUrl,
      development = :devMode,
      lastDisconnect = :lastDisconnect`,
    ExpressionAttributeNames: {
      "#name": 'name',
      "#type": 'type',
      "#connected": 'connected',
      "#url": 'url',
      "#lang": 'language',
      "#version": 'version',
    },
    ExpressionAttributeValues: {
      ":name": data.name,
      ":nodes": {},
      ":type": `cloud`,
      ":timeAdded": +Date.now(),
      ":isyUsername": data.isyUsername,
      ":isyPassword": data.isyPassword,
      ":isConnected": false,
      ":worker": worker.deployment.metadata.name,
      ":netInfo": {publicIp: PARAMS.NS_PUBLIC_IP, publicPort: worker.nodePort, httpsIngress: worker.httpsIngress ? `${PARAMS.HTTPSINGRESS}/ns/${worker.deployment.metadata.name}/` : false},
      ":url": data.url,
      ":lang": data.language,
      ":version": data.version,
      ":oauth": data.oauth,
      ":userId": fullMsg.userId,
      ":timeStarted": 0,
      ":isyVersion": data.isyVersion,
      ":shortPoll": 60,
      ":longPoll": 120,
      ":customParams": {},
      ":customData": {},
      ":notices": {},
      ":logBucket": PARAMS.LOG_BUCKET,
      ":firstRun": true,
      ":pgUrl": worker.pgUrl,
      ":devMode": data.development || false,
      ":lastDisconnect": 0
    },
    ReturnValues: 'ALL_NEW'
  }
  try {
    let response = await DYNAMO.update(params).promise()
    if (response.hasOwnProperty('Attributes')) {
      let update = response.Attributes
      LOGGER.debug(`createNS: Created NodeServer (${update.profileNum}) ${update.name}`, fullMsg.userId)
      return update
    }
  } catch (err) {
    LOGGER.error(`createNS: ${err.stack}`, fullMsg.userId)
  }
}

// Command Methods
async function addNodeServer(cmd, fullMsg) {
  let data = fullMsg[cmd]
  data['isyUsername'] = 'pgc'
  data['isyPassword'] = randomAlphaOnlyString(10)
  await mqttSend(`${STAGE}/isy`, {
    id: data.id,
    topic: `${STAGE}/workers`,
    addNodeServer: data
  }, fullMsg)
}

async function resultAddNodeServer(cmd, fullMsg) {
  // Successfully added to ISY
  if (fullMsg.result.success) {
    LOGGER.info(`NodeServer added Successfully. Provisioning. This could take serveral minutes.`, fullMsg.userId)
    // Create Swarm Service
    let worker = await createService(cmd, fullMsg)
    if (worker) {
      let update = await createNS(cmd, fullMsg, worker)
      if (update) {
        LOGGER.info(`Provisioning successful for ${fullMsg[cmd].name}. Starting NodeServer.`, fullMsg.userId)
      }
    } else {
      LOGGER.error(`Failed to provision worker service. Removing NodeServer from ISY.`, fullMsg.userId)
      await removeNodeServer('removeNodeServer', {
        userId: fullMsg.userId,
        removeNodeServer: {
          profileNum: fullMsg[cmd].profileNum,
          id: fullMsg[cmd].id,
          isyVersion: fullMsg[cmd].isyVersion
        }
      })
    }
    await updateClientNodeServers(fullMsg[cmd].id, fullMsg)
  } else {
    try {
      LOGGER.error(`Failed to add NodeServer to ISY. Removing from DB. ${fullMsg.result.error}`, fullMsg.userId)
      let nodeServer = await deleteDbNodeServer(cmd, fullMsg)
      if (nodeServer.worker) {
        await mqttSend(`${STAGE}/ns/${nodeServer.worker}`, {
          id: nodeServer.worker,
          delete: {}
        }, fullMsg)
      }
    } catch (err) {
      LOGGER.error(`resultAddNodeServer: ${err.stack}`, fullMsg.userId)
    }
  }
}

async function removeNodeServer(cmd, fullMsg) {
  let data = fullMsg[cmd]
  await mqttSend(`${STAGE}/isy`, {
    id: data.id,
    topic: `${STAGE}/workers`,
    removeNodeServer: data
  }, fullMsg)
}

async function resultRemoveNodeServer(cmd, fullMsg) {
  if (fullMsg.result.success || fullMsg.result.statusCode === 404) {
    let nodeServer = await deleteDbNodeServer(cmd, fullMsg)
    if (nodeServer) {
      if (nodeServer.worker) {
        await mqttSend(`${STAGE}/ns/${nodeServer.worker}`, {
          id: nodeServer.worker,
          delete: {}
        }, fullMsg)
        LOGGER.info(`resultRemoveNodeServer: Sent stop to ${nodeServer.name} worker: ${nodeServer.worker}`, fullMsg.userId)
        await timeout(2000)
        await removeDeployment(cmd, fullMsg, nodeServer.worker)
        if (nodeServer.netInfo.publicPort !== 0) {
          await removeService(cmd, fullMsg, `${nodeServer.worker}-in`)
        }
        if (nodeServer.netInfo.httpsIngress) {
          await removeService(cmd, fullMsg, nodeServer.worker)
          await removeIngress(cmd, fullMsg, nodeServer.worker)
        }
      }
      LOGGER.info(`resultRemoveNodeServer: Removed ${nodeServer.name}(${nodeServer.worker}) successfully.`, fullMsg.userId)
      await updateClientNodeServers(fullMsg[cmd].id, fullMsg)
    }
  }
}

async function startNodeServer(cmd, fullMsg) {
  let data = fullMsg[cmd]
  try {
    let nodeServer = data.ns
    if (nodeServer && nodeServer.type && nodeServer.type === 'cloud') {
      if (nodeServer.isConnected) {
        return LOGGER.error(`${nodeServer.name} is already connected. Not sending start command.`, fullMsg.userId)
      }
      const getDeployment = await KUBERNETES.apis.apps.v1beta1.namespaces('nodeservers').deployments(nodeServer.worker).get()
      if (getDeployment.statusCode !== 200) {
        return LOGGER.error(`${nodeServer.name} couldn't get status of deployment.`, fullMsg.userId)
      }
      if (getDeployment.body.spec.replicas === 1) {
        return LOGGER.error(`${nodeServer.name} is already started. Not sending start command.`, fullMsg.userId)
      }
      let updateManifest = { spec: { replicas: 1 }}
      await KUBERNETES.apis.apps.v1.namespaces('nodeservers').deployments(nodeServer.worker).patch({ body: updateManifest })
      LOGGER.info(`${cmd} sent successfully. Starting ${nodeServer.name}`, fullMsg.userId)
    } else {
      LOGGER.error(`${nodeServer.name} not found or not a Cloud NodeServer`, fullMsg.userId)
    }
  } catch (err) {
    LOGGER.error(`startNodeServer ${err.satck}`, fullMsg.userId)
  }
}

async function stopNodeServer(cmd, fullMsg) {
  let data = fullMsg[cmd]
  try {
    let nodeServer = data.ns
    if (nodeServer && nodeServer.type && nodeServer.type === 'cloud') {
      const getDeployment = await KUBERNETES.apis.apps.v1beta1.namespaces('nodeservers').deployments(nodeServer.worker).get()
      if (getDeployment.statusCode !== 200) {
        return LOGGER.error(`${nodeServer.name} couldn't get status of deployment.`, fullMsg.userId)
      }
      if (getDeployment.body.spec.replicas === 0) {
        return LOGGER.error(`${nodeServer.name} is already stopped. Not sending stop command.`, fullMsg.userId)
      }
      let payload = {stop: ''}
      await mqttSend(`${STAGE}/ns/${nodeServer.worker}`, payload)
      LOGGER.info(`${cmd} sent successfully. Delaying 2 seconds before shutdown for NodeServer self cleanup.`), fullMsg.userId
      await timeout(2000)
      let updateManifest = { spec: { replicas: 0 }}
      await KUBERNETES.apis.apps.v1.namespaces('nodeservers').deployments(nodeServer.worker).patch({ body: updateManifest })
      LOGGER.info(`${cmd} sent successfully. Stopping ${nodeServer.name}`, fullMsg.userId)
    } else {
      LOGGER.error(`${nodeServer.name} not found or not a Cloud NodeServer`, fullMsg.userId)
    }
  } catch (err) {
    LOGGER.error(`stopNodeServer ${err.stack}`, fullMsg.userId)
  }
}

async function startLogStream(cmd, fullMsg) {
  const deployment = fullMsg[cmd].ns
  const logTopic = fullMsg.topic
  if (!LOGSTREAMS.hasOwnProperty(deployment)) {
    let podSpec = await KUBERNETES.api.v1.namespace('nodeservers').pods.get({
      qs: {
        labelSelector: `nodeserver=${deployment}`
      }
    })
    if (podSpec && (podSpec.body.items.length > 0)) {
      let pod = podSpec.body.items[0].metadata.name
      let logData = await KUBERNETES.api.v1.namespace('nodeservers').pods(pod).log.get({
        qs: {
          tailLines: 2500
        }
      })
      if (logData && logData.body) {
        //let logParts = logData.body.match(/(?=[\s\S])(?:.*(\n|\r)?){1,5000}/g).filter(Boolean)
        // LOGGER.debug(`startLogStream: ${logParts.length}`, fullMsg.userId)
        //let i = 0
        //for (let part of logParts) {
        //  i++
        let binaryString = PAKO.deflate(logData.body, {to: 'string', level: 9})
        //LOGGER.debug(`startLogStream: Compressed Log Part ${i} of ${logParts.length} Size: ${Buffer.byteLength(binaryString) / 1024}kb`, fullMsg.userId)
        let iotMessage = {
          topic: `${logTopic}/file`,
          payload: JSON.stringify({
            file: true,
            log: binaryString,
            end: true
          }),
          qos: 0
        }
        await IOT.publish(iotMessage).promise()
        // }
      }
      LOGSTREAMS[deployment] = await KUBERNETES.api.v1.namespace('nodeservers').pods(pod).log.getStream({
        qs: {
          follow: true,
          tailLines: 0,
          //previous: true
        }
      })
      LOGSTREAMS[deployment].on('data', async (data) => {
        let msg = {log: data.toString('utf8').replace(/\n/g, '')}
        await mqttSend(logTopic, msg, fullMsg)
      })
    }
  } else {
    LOGGER.error(`startLogStream: log already streaming`, fullMsg.userId)
  }
}

async function stopLogStream(cmd, fullMsg) {
  const deployment = fullMsg[cmd].ns
  if (LOGSTREAMS.hasOwnProperty(deployment)) {
    LOGSTREAMS[deployment].abort()
    delete LOGSTREAMS[deployment]
  }
}

// MQTT Methods
async function mqttSend(topic, message, fullMsg = {}, qos = 0) {
  if (fullMsg.userId) {
    message.userId = fullMsg.userId
  }
  const payload = JSON.stringify(message)
  const iotMessage = {
    topic: topic,
    payload: payload,
    qos: qos
  }
  return IOT.publish(iotMessage).promise()
}

// API
const checkCommand = (command) => apiSwitch[command] || null

const apiSwitch = {
  addNodeServer: {
    props: ['id', 'profileNum', 'url', 'name', 'language', 'isyVersion'],
    func: addNodeServer,
    result: resultAddNodeServer,
    type: null
  },
  removeNodeServer: {
    props: ['id', 'profileNum'],
    func: removeNodeServer,
    result: resultRemoveNodeServer,
    type: null
  },
  startNodeServer: {
    props: ['profileNum', 'isy.id', 'ns.worker'],
    func: startNodeServer,
    type: null
  },
  stopNodeServer: {
    props: ['profileNum', 'isy.id', 'ns.worker'],
    func: stopNodeServer,
    type: null
  },
  test: {
    props: [],
    func: createService,
    type: null
  },
  startLogStream: {
    props: ['ns'],
    func: startLogStream,
    type: null
  },
  stopLogStream: {
    props: ['ns'],
    func: stopLogStream,
    type: null
  }
}

const propExists = (obj, path) => {
  return !!path.split(".").reduce((obj, prop) => {
      return obj && obj[prop] ? obj[prop] : undefined;
  }, obj)
}

const verifyProps = (message, props) => {
  let confirm = {
    valid: true,
    missing: null
  }
  for (let prop of props) {
    if (!propExists(message, prop)) {
      confirm.valid = false
      confirm.missing = prop
      break
    }
  }
  return confirm
}

// Helper Methods
function randomAlphaOnlyString (length) {
  let text = ''
  const possible = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXTZabcdefghiklmnopqrstuvwxyz'
  for (let i = 0; i < length; i++) {
      text += possible.charAt(Math.floor(Math.random() * possible.length))
  }
  return text
}

const timeout = ms => new Promise(run => setTimeout(run, ms))

const btoa = function(str) { return Buffer.from(str, 'utf8').toString('base64') }
const atob = function(b64Encoded) { return Buffer.from(b64Encoded, 'base64').toString('utf8') }
const urlEncode = function(str) { return str.replace(/\+/g, '-').replace(/\//g, '_').replace(/\=+$/, '') }
const urlDecode = function(str) {
  str = (str + '===').slice(0, str.length + (str.length % 4))
  return str.replace(/-/g, '+').replace(/_/g, '/')
}

// Message Functions
async function processMessage(message) {
  let props = verifyProps(message, ['userId', 'topic'])
  if (!props.valid) {
    return LOGGER.error(`Request missing required property: ${props.missing} :: ${JSON.stringify(message)}`)
  }
  LOGGER.debug(JSON.stringify(message))
  if (message.hasOwnProperty('result')) {
    for (let key in message) {
      if (['topic', 'userId', 'result'].includes(key)) { continue }
      try {
        let command = checkCommand(key)
        if (!command) { continue }
        LOGGER.debug(`Processing results for ${key}...`, message.userId)
        await command.result(key, message)
      } catch (err) {
        LOGGER.error(`${key} result error :: ${err.stack}`, message.userId)
      }
    }
  } else {
    for (let key in message) {
      if (['userId', 'topic'].includes(key)) { continue }
      try {
        let command = checkCommand(key)
        if (!command) { continue }
        let props = verifyProps(message[key], apiSwitch[key].props)
        if (!props.valid) {
          return LOGGER.error(`${key} was missing ${props.missing} :: ${JSON.stringify(message)}`, message.userId)
        }
        LOGGER.debug(`Processing command ${key}...`, message.userId)
        await command.func(key, message)
      } catch (err) {
        LOGGER.error(`${key} error :: ${err.stack}`, message.userId)
      }
    }
  }
}

async function getMessages() {
  const params = {
    MaxNumberOfMessages: 10,
    QueueUrl: PARAMS.SQS_WORKERS,
    WaitTimeSeconds: 10
  }
  let deletedParams = {
    Entries: [],
    QueueUrl: PARAMS.SQS_WORKERS
  }
  try {
    LOGGER.info(`Getting messages...`)
    let data = await SQS.receiveMessage(params).promise()
    if (data.Messages) {
        LOGGER.info(`Got ${data.Messages.length} message(s)`)
        let tasks = []
        for (let message of data.Messages) {
          try {
            let body = JSON.parse(message.Body)
            let msg = body.msg
            LOGGER.info(`Got Message: ${JSON.stringify(msg)}`)
            tasks.push(processMessage(msg))
          } catch (err) {
            LOGGER.error(`Message not JSON: ${message.Body}`)
          }
          deletedParams.Entries.push({
              Id: message.MessageId,
              ReceiptHandle: message.ReceiptHandle
          })
        }
        let results = []
        for (let task of tasks) {
          results.push(await task)
        }
        let deleted = await SQS.deleteMessageBatch(deletedParams).promise()
        deletedParams.Entries = []
        LOGGER.info(`Deleted Messages: ${JSON.stringify(deleted)}`)
    } else {
      LOGGER.info(`No messages`)
    }
  } catch (err) {
    LOGGER.error(err.stack)
  }
}

async function getParameters(nextToken) {
  const ssm = new AWS.SSM()
  var ssmParams = {
    Path: `/pgc/${STAGE}/`,
    MaxResults: 10,
    Recursive: true,
    NextToken: nextToken,
    WithDecryption: true
  }
  try {
    let params = await ssm.getParametersByPath(ssmParams).promise()
    if (params.Parameters.length === 0) throw new Error(`Parameters not retrieved. Exiting.`)
    for (let param of params.Parameters) {
      PARAMS[param.Name.split('/').slice(-1)[0]] = param.Value
    }
    if (params.hasOwnProperty('NextToken')) {
      await getParameters(params.NextToken)
    }
  } catch (err) {
    LOGGER.error(`getParameters: ${err.stack}`)
    process.exit(1)
  }
}

async function startHealthCheck() {
  require('http').createServer(function(request, response) {
    if (request.url === '/health' && request.method ==='GET') {
        //AWS ELB pings this URL to make sure the instance is running smoothly
        let data = JSON.stringify({uptime: process.uptime()})
        response.writeHead(200, {'Content-Type': 'application/json'})
        response.write(data)
        response.end()
    }
  }).listen(3000)
}

async function main() {
  await getParameters()
  if (!PARAMS.SQS_WORKERS) {
    LOGGER.error(`No Queue retrieved. Exiting.`)
    process.exit(1)
  }
  LOGGER.info(`Retrieved Parameters from AWS Parameter Store for Stage: ${STAGE}`)
  await configAWS()
  startHealthCheck()
  // fromKubeconfig(null, 'pgc.nonprod.isy.io')
  // getInCluster()
  const backend = new Request(process.env.LOCAL ? Request.config.fromKubeconfig(null, 'pgc.nonprod.isy.io') : Request.config.getInCluster())
  KUBERNETES = new Client({ backend })
  await KUBERNETES.loadSpec()
  try {
    while (true) {
      await getMessages()
    }
  } catch(err) {
    LOGGER.error(err.stack)
    main()
  }
}

['SIGINT', 'SIGTERM'].forEach(signal => {
  process.on(signal, () => {
    LOGGER.debug('Shutdown requested. Exiting...')
    setTimeout(() => {
      process.exit()
    }, 500)
  })
})

main()