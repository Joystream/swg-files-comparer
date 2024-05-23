import * as fs from 'fs'
import fetch from 'node-fetch'
import { getFileBirthtime, sortFiles } from './utils/utils.js'
import psp from 'prompt-sync-plus'
import { format } from 'date-fns'
import {
  FILE_BASE_PATH,
  getAllLocalFilePath,
  getAllRemoteFilePath,
  getCheckResultPath,
  getDiffBagPath,
  getDiffPath,
  getFilePathWithPostfix,
  getFilePostfix,
  getIncValue,
  getLocalFilePath,
  getRemoteBagPath,
  getRemoteFilePath,
  setLocalFilePath,
} from './utils/fs.js'
import {
  ACTIVE_BUCKET_METADATA,
  ALL_STORAGE_BAGS_OBJECTS_QUERY,
  ALL_STORAGE_OPERATORS,
  BUCKETS_ASSIGNED_STORAGE_BAGS,
  STORAGE_BAGS_OBJECTS_QUERY,
  STORAGE_BAGS_QUERY,
} from './api/queries.js'
import {
  ActiveBucketMetadataResponse,
  BagResult,
  BagWithObjects,
  Commands,
  StorageBuckets,
  StorageBucketWithBags,
  StorageObject,
} from './types.js'

const RECENT_FILES_THRESHOLD = 1000 * 60 * 20 // 20 minutes

const prompt = psp(undefined)

const headRequestAsset = async (baseUrl: string, objectId: string) => {
  const url = `${baseUrl}/${objectId}`

  let code = 404
  try {
    const response = await fetch(url, {
      method: 'HEAD',
      headers: { 'Content-Type': 'application/json' },
    })

    if (response.status) {
      code = response.status
    } else {
      return undefined
    }
    return code
  } catch (err) {
    console.warn(`Request for ${objectId} to ${baseUrl} failed with ${err}`)
    return undefined
  }
}

const fetchPaginatedData = async <T>(
  query: string,
  variables: object,
  pageSize: number,
  keyOverwrite?: string,
  logProgress?: boolean
): Promise<T[]> => {
  const controller = new AbortController()
  let hasMoreData = true
  let offset = 0
  let data: T[] = []
  const key = keyOverwrite || Object.keys(variables)[0]
  let retryCount = 0

  while (hasMoreData) {
    const timeoutId = setTimeout(() => controller.abort(), 1000 * 60 * 5) // 5 minutes
    logProgress && console.log('Fetching data. Page:', offset / pageSize)
    const response = await fetch('https://query.joystream.org/graphql', {
      method: 'POST',
      signal: controller.signal,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: query,
        variables: { ...variables, limit: pageSize, offset: offset },
      }),
    })

    clearTimeout(timeoutId)

    if (!response.ok) {
      const error = await response.text()
      console.log(error)

      if (retryCount < 5) {
        console.log('Retrying... Retry count:', retryCount)
        retryCount++
        continue
      } else {
        throw new Error(`Error fetching data: ${response.statusText}`)
      }
    }

    const jsonResponse: any = await response.json()

    data = data.concat(jsonResponse.data[key])
    hasMoreData = jsonResponse.data[key].length === pageSize

    offset += pageSize
  }

  return data
}

const getBucketObjects = async (id?: string) => {
  const bucketId = id ? id : prompt('Enter bucket id: ')

  if (!bucketId || isNaN(parseInt(bucketId))) {
    console.log('Please provide a bucket id')
    process.exit(1)
  }
  const bagId = prompt('Enter bag id (optional):')
  const timeRange = prompt('Enter start and end time (optional, format: startTimestamp-endTimestamp):');

  let startTime, endTime;
  if (timeRange) {
    customTimestamp = true
    const timestamps = timeRange.split('-');
    startTime = timestamps[0] ? Number(timestamps[0]) : undefined;
    endTime = timestamps[1] ? Number(timestamps[1]) : undefined;
  }

  if (bagId && isNaN(parseInt(bagId))) {
    console.log('If you want to get only a single bag, provide only the number from the id, dynamic:channel:XXX')
    process.exit(1)
  }

  console.log('Getting bags...')
  const allBucketBags = await fetchPaginatedData<StorageBucketWithBags>(
    STORAGE_BAGS_QUERY,
    { storageBucket: bucketId },
    3000,
    'storageBags'
  )
  const bucketBags = bagId != null ? allBucketBags.filter((bag) => bag.id.includes(bagId)) : allBucketBags
  console.log(`Found ${bucketBags.length} bags`)
  const bucketBagsIds = bucketBags.map((bag) => bag.id)

  console.log('Getting objects... This can take 10-30 mins to execute, please be patient.')
  const bagObjectsMap: {
    [key: string]: string[]
  } = {}
  const BATCH_SIZE = 1000
  for (let i = 0; i < bucketBagsIds.length; i += BATCH_SIZE) {
    const storageBags = await fetchPaginatedData<BagWithObjects>(
      STORAGE_BAGS_OBJECTS_QUERY,
      {
        storageBags: bucketBagsIds.slice(i, i + BATCH_SIZE),
        startTimestamp: startTime ? new Date(startTime) : new Date(new Date().getTime() - RECENT_FILES_THRESHOLD),
        endTimestamp: endTime ? new Date(endTime) : undefined,
      },
      BATCH_SIZE,
      'storageBags'
    )

    const filteredBags = storageBags.map((bag) => ({ ...bag, objects: bag.objects.filter((obj) => obj.isAccepted) }))
    const acceptedObjectsNumber = filteredBags.map((bag) => bag.objects.length).reduce((acc, val) => acc + val, 0)
    console.log('i:', i, ' accepted:', acceptedObjectsNumber)

    filteredBags.forEach((bag) => {
      const bagId = bag.id
      bagObjectsMap[bagId] = bag.objects.map((obj) => obj.id)
    })
  }

  const totalObjectsCount = Object.values(bagObjectsMap).flat().length
  console.log(`Found ${totalObjectsCount} accepted objects`)
  await fs.promises.writeFile(
    bagId !== null ? getRemoteBagPath(bagId, filesPostfix) : getRemoteFilePath(filesPostfix),
    JSON.stringify(bagObjectsMap)
  )
}

const getLocalFiles = async () => {
  const dirPath = prompt('Enter path to your local files directory: ')
  if (!dirPath) {
    console.log('Path is incorrect or not provided')
    process.exit(1)
  }

  console.log('Getting files...')
  const ts = new Date().getTime()
  const allFiles = await fs.promises.readdir(dirPath)
  const acceptedFiles = allFiles.filter((file) => {
    !isNaN(parseInt(file))
  })
  console.log(`Found ${acceptedFiles.length} files.`)
  const sortedFiles = sortFiles(acceptedFiles)
  await fs.promises.writeFile(setLocalFilePath(nextInc), JSON.stringify(sortedFiles))
}

const getDifferences = async (locals?: string[], remotes?: { [key: string]: StorageObject[] }) => {
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

const getMissing = async () => {
  const customPath = prompt('Enter path to a diff file (optional):')
  const ignoreProvidersInput = prompt('Enter providers to ignore (optional) e.g "1,3,5":')
  let path = getDiffPath(filesPostfix)
  let providerInputs = undefined

  const ignoreProviders: number[] = []
  if (customPath) {
    if (customPath.includes('.json')) {
      path = customPath
    } else {
      providerInputs = customPath
    }
  }
  if (ignoreProvidersInput) {
    providerInputs = ignoreProvidersInput
  }
  if (providerInputs) {
    try {
      providerInputs.split(',').forEach((sp) => ignoreProviders.push(parseInt(sp)))
    } catch (err) {
      console.log(`Invalid input for providers, use format 1 or 0,1,4. Err: ${err}`)
      process.exit(1)
    }
  }

  console.log(`Checking missing objects from ${path}...`)
  const missingObjects = JSON.parse(await fs.promises.readFile(path, 'utf-8')).missingObjectsPerBag

  const bagsWithMissingObjects = Object.keys(missingObjects)

  const BATCH_SIZE = 2
  const storageProvidersToCheck: string[] = []
  const bucketsForBags: { [key: string]: string[] } = {}
  for (let i = 0; i < bagsWithMissingObjects.length; i += BATCH_SIZE) {
    const storageProvidersAssigned = await fetchPaginatedData<{ id: string; storageBuckets: { id: string }[] }>(
      BUCKETS_ASSIGNED_STORAGE_BAGS,
      { storageBags: bagsWithMissingObjects.slice(i, i + BATCH_SIZE) },
      BATCH_SIZE
    )

    for (let bag of storageProvidersAssigned) {
      const sps = []
      for (let sp of bag.storageBuckets) {
        if (!ignoreProviders.includes(+sp.id)) {
          sps.push(sp.id)
          if (!storageProvidersToCheck.includes(sp.id)) {
            storageProvidersToCheck.push(sp.id)
          }
        }
      }
      bucketsForBags[bag.id] = sps
    }
  }

  storageProvidersToCheck.sort((a, b) => parseInt(a) - parseInt(b))
  const storageProvidersHoldingMissingBags = await fetchPaginatedData<ActiveBucketMetadataResponse>(
    ACTIVE_BUCKET_METADATA,
    { storageBuckets: storageProvidersToCheck },
    1000
  )
  storageProvidersHoldingMissingBags.sort((a, b) => parseInt(a.id) - parseInt(b.id))
  const operatingProviders: { [key: string]: string } = {}
  for (let sp of storageProvidersHoldingMissingBags) {
    if (sp.operatorStatus.__typename === 'StorageBucketOperatorStatusActive') {
      const endpoint = sp.operatorMetadata.nodeEndpoint
      if (endpoint && endpoint.length) {
        endpoint.toString().endsWith('/')
        if (endpoint.toString().endsWith('/')) {
          operatingProviders[sp.id] = `${sp.operatorMetadata.nodeEndpoint}api/v1/files`
        } else {
          operatingProviders[sp.id] = `${sp.operatorMetadata.nodeEndpoint}/api/v1/files`
        }
      }
    }
  }

  const results = []
  let foundCount = 0
  let triedCount = 0
  for (let bagId of bagsWithMissingObjects) {
    const assignedSps = bucketsForBags[bagId]
    const missingObjectsInBag = missingObjects[bagId]
    const bagResult: BagResult = {
      storageBag: bagId,
      operatorsChecked: assignedSps,
      dataObjects: missingObjectsInBag,
      granularResults: [],
      foundObjects: 0,
      foundObjectsUrls: [],
    }
    triedCount += missingObjectsInBag.length
    for (let dataObjectId of missingObjectsInBag) {
      const objectResult = []
      let found = false
      for (let sp of assignedSps) {
        const providerUrl = operatingProviders[sp]
        const res = await headRequestAsset(providerUrl, dataObjectId)
        if (res) {
          objectResult.push({
            bucketId: sp,
            result: res,
          })
          if (res == 200) {
            found = true
            bagResult.foundObjectsUrls.push(`${providerUrl}/${dataObjectId}`)
            console.log(
              `Object ID ${dataObjectId} in bag ${bagId} is available in bucket ${sp} at ${providerUrl}/${dataObjectId}.`
            )
            break
          }
        } else {
          objectResult.push({
            bucketId: sp,
            result: 'not reached',
          })
        }
      }
      bagResult.granularResults.push(objectResult)
      if (found) {
        bagResult.foundObjects++
        foundCount++
      }
    }
    results.push(bagResult)
  }
  console.log(`A total of ${foundCount} out of ${triedCount} objects presumed lost were found.`)
  await fs.promises.writeFile(getCheckResultPath(filesPostfix), JSON.stringify(results, null, 2))
}

const downloadMissing = async () => {
  const diff = JSON.parse(await fs.promises.readFile(getDiffPath(filesPostfix), 'utf-8'))
  const urls = diff.map((bagResult: BagResult) => bagResult.foundObjectsUrls).flat()
  if (urls.length === 0) {
    console.log('No missing objects found')
    process.exit(1)
  }
  console.log(`Downloading ${urls.length} missing objects...`)
  urls.forEach(async (url: string) => {
    const fileName = url.split('/').slice(-1)[0]
    await fetch(url).then((res) => {
      const dest = fs.createWriteStream(`${FILE_BASE_PATH}/${fileName}`)
      res?.body?.pipe(dest)
    })
  })
}

const checkAllOperators = async () => {
  const allOperators = await fetchPaginatedData<StorageBuckets>(ALL_STORAGE_OPERATORS, {}, 1000, 'storageBuckets')
  const objectIds: string[] = []
  for (let index = 0; index < allOperators.length; index++) {
    const operator = allOperators[index]
    console.log(
      `Checking operator ${operator.id} at ${operator.operatorMetadata.nodeEndpoint} (${index + 1} of ${
        allOperators.length
      })`
    )
    console.log('url', operator.operatorMetadata.nodeEndpoint.split('/').slice(0, -2).join('/'))
    const bucketFiles = await checkRemoteNode(operator.operatorMetadata.nodeEndpoint.split('/').slice(0, -2).join('/'))
    if (!bucketFiles) {
      continue
    }
    const filesSet = new Set(Object.values(bucketFiles).flat())

    // spread exceeds the stack size unfortunately
    filesSet.forEach((file) => objectIds.push(file))
  }
  console.log('All operators checked')
  const allFilesSet = new Set(objectIds)
  fs.promises.writeFile(getAllLocalFilePath(filesPostfix), JSON.stringify([...allFilesSet]))

  console.log('Getting objects... This will take 1+hr and ~600 requests of data, please be patient.')
  const data = await fetchPaginatedData<BagWithObjects>(
    ALL_STORAGE_BAGS_OBJECTS_QUERY,
    {}, // { startTimestamp: new Date(new Date().getTime() - RECENT_FILES_THRESHOLD) },
    100,
    'storageBags',
    true
  )
  const bagToObjectsMap: { [key: string]: StorageObject[] } = {}
  data.forEach((bag) => {
    bagToObjectsMap[bag.id] = bag.objects
  })
  fs.promises.writeFile(getAllRemoteFilePath(filesPostfix), JSON.stringify(bagToObjectsMap))

  getDifferences([...allFilesSet], bagToObjectsMap)
}

const manualHeadRequest = async () => {
  const url = prompt('Enter request url: ')
  const providerUrl = `${url.split('files/')[0]}files`
  const objectId = url.split('files/')[1]
  if (!providerUrl || !objectId) {
    console.log('Input must be head <URL/api/v1/files/> <objectId>')
    process.exit(1)
  }

  try {
    const res = await headRequestAsset(url, objectId)
    console.log('Res', res)
  } catch (err) {
    console.log('err', err)
  }
}

const checkRemoteNode = async (url?: string) => {
  let endpoint = url || ''
  if (!endpoint) {
    try {
      endpoint = prompt('Enter remote node endpoint: ')
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

const command = prompt('Enter command:\n(command list is available in readme)\n').toLowerCase()
const nextInc = await getIncValue()
const filesPostfix = await getFilePostfix()
let customTimestamp = false

switch (command) {
  case Commands.LocalFiles:
    getLocalFiles()
    break
  case Commands.BucketObjects:
    getBucketObjects()
    break
  case Commands.Diff:
    getDifferences()
    break
  case Commands.CheckMissing:
    getMissing()
    break
  case Commands.CheckNode:
    await checkRemoteNode()
    await getBucketObjects()
    await getDifferences()
    break
  case Commands.Head:
    manualHeadRequest()
    break
  case Commands.RemoteNode:
    checkRemoteNode()
    break
  case Commands.DownloadMissing:
    downloadMissing()
    break
  case Commands.CheckAllOperators:
    checkAllOperators()
    break
  default:
    console.log('Unknown command')
    process.exit(1)
}
