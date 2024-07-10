import dotenv from 'dotenv'
import { createResultsFolder, getFilePostfix, getIncValue } from './utils/fs.js'
import { Commands } from './types.js'
import {
  checkAllOperators,
  checkRemoteNode,
  downloadMissing,
  getBucketObjects,
  getDifferences,
  getLocalFiles,
  getMissing,
  manualHeadRequest,
} from './commands/index.js'
import {prompt} from "./utils/prompt.js";
import { store } from './store.js'

dotenv.config()

await createResultsFolder()
let isRunning = true
const command = prompt('Enter command:\n(command list is available in readme)\n').toLowerCase()
const nextInc = await getIncValue()
const filesPostfix = await getFilePostfix()
store.setState('nextInc', nextInc)
store.setState('filesPostfix', filesPostfix)
store.setState('customTimestamp', false)

while (isRunning) {
  switch (command) {
    case Commands.LocalFiles:
      await getLocalFiles()
      break
    case Commands.BucketObjects:
      await getBucketObjects()
      break
    case Commands.Diff:
      await getDifferences()
      break
    case Commands.CheckMissing:
      await getMissing()
      break
    case Commands.CheckNode:
      await checkRemoteNode()
      await getBucketObjects()
      await getDifferences()
      break
    case Commands.Head:
      await manualHeadRequest()
      break
    case Commands.RemoteNode:
      await checkRemoteNode()
      break
    case Commands.DownloadMissing:
      await downloadMissing()
      break
    case Commands.CheckAllOperators:
      await checkAllOperators()
      break
    case Commands.Exit:
      isRunning = false
      break
    default:
      console.log('Unknown command')
    // process.exit(1)
  }
}
