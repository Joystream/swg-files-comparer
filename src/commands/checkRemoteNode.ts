import fetch from 'node-fetch'
import fs from 'fs'
import { setLocalFilePath } from '../utils/fs.js'
import { store } from '../store.js'
import { prompt } from '../utils/prompt.js'

export const checkRemoteNode = async (url?: string) => {
  const nextInc = store.getState('nextInc')
  let endpoint = url || ''
  if (!endpoint) {
    try {
      endpoint = prompt('Enter remote node endpoint: ') || ''
    } catch (err) {
      console.log('Entered endpoint is not valid')
      process.exit(1)
    }
  }
  const localFilesEndpoint = endpoint + '/storage/api/v1/state/data-objects'
  const nodeName = new URL(endpoint).hostname.replaceAll('.', '-')
  const res = await fetch(localFilesEndpoint)
    .then((res) => res.json())
    .then((json) => {
      if (url) {
        return json
      } else {
        fs.promises.writeFile(setLocalFilePath(nextInc, nodeName), JSON.stringify(json))
      }
    })
  return res ? res : null
}
