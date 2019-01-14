'use strict'

const STAGE = process.env.STAGE || 'test'
const LOCAL = process.env.LOCAL || false
const DYNAMO_NS = `pg_${STAGE}-nsTable`
const SQS_WORKER = `pg_${STAGE}-workers`
const SECRETS = require('./secrets')
const AWS_ACCESS_KEY_ID = SECRETS.get('SWARM_AWS_ACCESS_KEY_ID')
const AWS_SECRET_ACCESS_KEY = SECRETS.get('SWARM_AWS_SECRET_ACCESS_KEY')
if (AWS_ACCESS_KEY_ID && AWS_SECRET_ACCESS_KEY) {
  process.env['AWS_ACCESS_KEY_ID'] = AWS_ACCESS_KEY_ID
  process.env['AWS_SECRET_ACCESS_KEY'] = AWS_SECRET_ACCESS_KEY
}

const fs = require('fs')
const Docker = require('dockerode')
const AWS = require('aws-sdk')
AWS.config.update({region:'us-east-1', correctClockSkew: true})
const SQS = new AWS.SQS()
const SSM = new AWS.SSM()
const DYNAMO = new AWS.DynamoDB.DocumentClient()

let PARAMS = {}

const DOCKER_HOST = process.env.DOCKER_HOST || '18.207.110.225'
const DOCKER_PORT = process.env.DOCKER_PORT || 2376
let DOCKER_CA, DOCKER_CERT, DOCKER_KEY, IOT
try {
  DOCKER_CA = fs.readFileSync(LOCAL ? `certs/${STAGE}/ca.pem` : `/run/secrets/ca.pem`)
  DOCKER_CERT = fs.readFileSync(LOCAL ? `certs/${STAGE}/cert.pem` : `/run/secrets/cert.pem`)
  DOCKER_KEY = fs.readFileSync(LOCAL ? `certs/${STAGE}/key.pem` : `/run/secrets/key.pem`)
} catch (err) {
  console.error(`Error loading docker certs for API. Exiting...`)
  process.kill(process.pid, 'SIGTERM')
}

console.log(`Worker Manager: Running in Stage: ${STAGE}`)
console.log(`ENV: ${JSON.stringify(process.env)}`)

async function configAWS() {
  IOT = new AWS.IotData({endpoint: `${PARAMS.IOT_ENDPOINT_HOST}`})
}

const DOCKER = new Docker({
  host: DOCKER_HOST,
  port: DOCKER_PORT,
  ca: DOCKER_CA,
  cert: DOCKER_CERT,
  key: DOCKER_KEY
})

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
// Create Swarm Service
async function createService(cmd, fullMsg) {
  try {
    let params = {
      userId: fullMsg.userId,
      id: fullMsg[cmd].id,
      profileNum: fullMsg[cmd].profileNum,
      password: fullMsg[cmd].isyPassword
    }
    let service = await DOCKER.createService({
      Name: `${fullMsg[cmd].name}_${fullMsg.userId}_${fullMsg[cmd].id.replace(/:/g, '')}_${fullMsg[cmd].profileNum}`,
      TaskTemplate: {
        ContainerSpec: {
          Image: "nodeserver:latest",
          Env: [
            `PGURL=https://pxu4jgaiue.execute-api.us-east-1.amazonaws.com/test/api/sys/nsgetioturl?params=${bota(JSON.stringify(params))}`
          ]
        },
        Resources: {
          Limits: {
            MemoryBytes: 67108864, // 64MB, 128MB = 134217728
            NanoCPUs: 250000000
          }
        },
        RestartPolicy: {
          "Condition": "on-failure",
          "Delay": 60000000000,
          "MaxAttempts": 0
        },
        LogDriver: {
          Name: "awslogs",
          Options: {
            "awslogs-region": "us-east-1",
            "awslogs-group": `/pgc/${STAGE}/nodeservers`,
            tag: "{{.Name}}-{{.ID}}"
          }
        },
      },
      Mode: {
        Replicated: {
          Replicas: 1
        }
      },
      Networks: [{
        Target: "pgc"
      }],
      EndpointSpec: {
        Ports: [{ TargetPort: 3000 }]
      }
    })
    return service
  } catch (err) {
    LOGGER.error(`createService: ${err.stack}`, fullMsg.userId)
  }
}

async function removeService(cmd, fullMsg, worker) {
  try {
    let service = await DOCKER.getService(worker)
    return await service.remove()
  } catch (err) {
    LOGGER.error(`removeService: ${err.stack}`, fullMsg.userId)
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

// Update Worker on NodeServer
async function updateNS(cmd, fullMsg) {
  let data = fullMsg[cmd]
  let params = {
    TableName: DYNAMO_NS,
    Key: {
      "id": data.isy.id,
      "profileNum": `${data.profileNum}`
    },
    UpdateExpression: `
      SET timeStarted = :timeStarted,
      connected = :connected`,
    ExpressionAttributeValues: {
      ":timeStarted": 0,
      ":connected": false
    },
    ReturnValues: 'ALL_NEW'
  }
  try {
    let data = await DYNAMO.update(params).promise()
    if (data.hasOwnProperty('Attributes')) {
      let update = data.Attributes
      LOGGER.debug(`updateNS: Updated NodeServer (${update.profileNum})${update.name} JSON: ${JSON.stringify(update)}`, fullMsg.userId)
      return update
    }
  } catch (err) {
    LOGGER.error(`updateNS: ${err.stack}`, fullMsg.userId)
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
      oauth = :oauth
      `,
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
      ":worker": worker.id,
      ":netInfo": {},
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
      ":logBucket": PARAMS.LOG_BUCKET
    },
    ReturnValues: 'ALL_NEW'
  }
  try {
    let workerInfo = await worker.inspect()
    params.ExpressionAttributeValues[":netInfo"] = {
      publicIp: DOCKER_HOST,
      publicPort: workerInfo.Endpoint.Ports[0].PublishedPort
    }
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
        LOGGER.info(`Provisioning successful for ${fullMsg[cmd].name}. Starting NodeServer.`)
      }
    } else {
      LOGGER.error(`Failed to provision worker service. Removing NodeServer from ISY.`)
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
        await mqttSend(`${STAGE}/workers/${nodeServer.worker}`, {
          id: nodeServer.worker,
          removeNodeServer: {}
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
  if (fullMsg.result.success) {
    let nodeServer = await deleteDbNodeServer(cmd, fullMsg)
    if (nodeServer) {
      if (nodeServer.worker) {
        await mqttSend(`${STAGE}/workers/${nodeServer.worker}`, {
          id: nodeServer.worker,
          removeNodeServer: {}
        }, fullMsg)
        LOGGER.info(`Sent stop to ${nodeServer.name} worker: ${nodeServer.worker}`, fullMsg.userId)
        await timeout(2000)
        await removeService(cmd, fullMsg, nodeServer.worker)
      }
      LOGGER.info(`Removed ${nodeServer.name}(${nodeServer.worker}) successfully.`, fullMsg.userId)
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
        LOGGER.error(`${nodeServer.name} is already connected. Not sending start command.`, fullMsg.userId)
      } else {
        let service = await DOCKER.getService(nodeServer.worker)
        let serviceInfo = await service.inspect()
        if (serviceInfo.Spec.Mode.Replicated.Replicas === 1) {
          LOGGER.error(`${nodeServer.name} is already started. Not sending start command.`, fullMsg.userId)
        } else {
          let serviceUpdate = serviceInfo.Spec
          serviceUpdate['id'] = serviceInfo.id,
          serviceUpdate['version'] = `${serviceInfo.Version.Index}`,
          serviceUpdate.Mode.Replicated.Replicas = 1
          await service.update(serviceUpdate)
          await updateNS(cmd, fullMsg)
          LOGGER.info(`${cmd} sent successfully. Starting ${nodeServer.name}`, fullMsg.userId)
        }
      }
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
      let service = await DOCKER.getService(nodeServer.worker)
      let serviceInfo = await service.inspect()
      if (serviceInfo.Spec.Mode.Replicated.Replicas === 0) {
        LOGGER.error(`${nodeServer.name} is already stopped. Not sending stop command.`, fullMsg.userId)
      } else {
        let serviceUpdate = serviceInfo.Spec
        serviceUpdate['id'] = serviceInfo.id,
        serviceUpdate['version'] = `${serviceInfo.Version.Index}`,
        serviceUpdate.Mode.Replicated.Replicas = 0
        let update = await updateNS(cmd, fullMsg)
        let payload = {[cmd]: update}
        await mqttSend(`${STAGE}/workers/${nodeServer.worker}`, payload)
        LOGGER.info(`${cmd} sent successfully. Delaying 2 seconds before shutdown for NodeServer self cleanup.`), fullMsg.userId
        await timeout(2000)
        await service.update(serviceUpdate)
        LOGGER.info(`${cmd} sent successfully. Stopping ${nodeServer.name}`, fullMsg.userId)
      }
    } else {
      LOGGER.error(`${nodeServer.name} not found or not a Cloud NodeServer`, fullMsg.userId)
    }
  } catch (err) {
    LOGGER.error(`stopNodeServer ${err.stack}`, fullMsg.userId)
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
  var ssmParams = {
    Path: `/pgc/${STAGE}/`,
    MaxResults: 10,
    Recursive: true,
    NextToken: nextToken,
    WithDecryption: true
  }
  try {
    let params = await SSM.getParametersByPath(ssmParams).promise()
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

async function main() {
  await getParameters()
  if (!PARAMS.SQS_WORKERS) {
    LOGGER.error(`No Queue retrieved. Exiting.`)
    process.exit(1)
  }
  LOGGER.info(`Retrieved Parameters from AWS Parameter Store for Stage: ${STAGE}`)
  await configAWS()
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