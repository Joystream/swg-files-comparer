import { StorageObject } from '../types.js'
import fs from 'fs'
import { getDiffBagPath, getDiffPath, getLocalFilePath, getRemoteBagPath, getRemoteFilePath } from '../utils/fs.js'
import {store} from "../store.js";
import {prompt} from "../utils/prompt.js";


export const getDifferences = async (
  locals?: string[],
  remotes?: { [key: string]: StorageObject[] }
) => {
  const filesPostfix = store.getState('filesPostfix')
  const customTimestamp = store.getState('customTimestamp')

  const bagId = locals ? null : prompt('Enter bag id (optional): ')

  if (bagId && isNaN(parseInt(bagId))) {
    console.log('If you want to diff only a single bag, provide only the number from the id, dynamic:channel:XXX')
    process.exit(1)
  }

  const localFiles: string[] =
    locals || JSON.parse(await fs.promises.readFile(getLocalFilePath(filesPostfix), 'utf-8')) || []
  const remoteFiles: { [key: string]: StorageObject[] } =
    remotes ||
    JSON.parse(
      await fs.promises.readFile(
        bagId != null ? getRemoteBagPath(bagId, filesPostfix) : getRemoteFilePath(filesPostfix),
        'utf-8'
      )
    )

  const localFilesSet = new Set(localFiles)
  const remoteApprovedSet = new Set(
    Object.values(remoteFiles)
      .map((objects) => objects.filter((obj) => obj.isAccepted).map((obj) => obj.id))
      .flat()
  )
  const remoteUnapprovedSet = new Set(
    Object.values(remoteFiles)
      .map((objects) => objects.filter((obj) => !obj.isAccepted).map((obj) => obj.id))
      .flat()
  )

  const unexpectedUnapproved = new Set([...localFilesSet].filter((id) => remoteUnapprovedSet.has(id)))
  const unexpectedLocal = new Set(
    [...localFilesSet].filter((id) => !remoteApprovedSet.has(id) && !unexpectedUnapproved.has(id))
  )

  const missingObjectsPerBag: { [bagId: string]: string[] } = {}
  Object.entries(remoteFiles).forEach(([bagId, objects]) => {
    const missingObjects = objects.filter(({ id }) => !localFilesSet.has(id)).map((object) => object.id)
    if (missingObjects.length !== 0) {
      missingObjectsPerBag[bagId] = missingObjects
    }
  })

  const missingObjects = new Set(Object.values(missingObjectsPerBag).flat())

  console.log(`Missing ${missingObjects.size} objects`)
  if (!customTimestamp) {
    console.log(`Found ${unexpectedLocal.size} unexpected local objects`)
    console.log(`Found ${unexpectedUnapproved.size} QN unapproved local objects`)
  }

  await fs.promises.writeFile(
    bagId != null ? getDiffBagPath(bagId, filesPostfix) : getDiffPath(filesPostfix),
    JSON.stringify({
      unexpectedLocal: [...unexpectedLocal],
      missingObjectsPerBag: missingObjectsPerBag,
    })
  )
}
