import * as fs from 'fs/promises'
import { PathLike } from 'node:fs'
import { getFileBirthtime } from './utils/utils'

const LOCAL_FILES_PATH = './local.json'
const REMOTE_FILES_PATH = './remote.json'
const RECENT_FILES_THRESHOLD = 1000 * 60 * 20 // 20 minutes
const getRemoteBagPath = (bagId: string) => `./remote-${bagId}.json`
const DIFF_PATH = './diff.json'
const getDiffBagPath = (bagId: string) => `./diff-${bagId}.json`
const CHECK_PATH = `./checked.json`

type StorageObject = {
  id: string
  isAccepted: boolean
  createdAt: string
}

type BagWithObjects = {
  id: string
  objects: StorageObject[]
}

type StorageBucketWithBags = {
  id: string
  storageBags: {
    id: string
  }[]
}

type ActiveBucketMetadataResponse = {
  id: string
  operatorStatus: {
    __typename: string
  }
  operatorMetadata: {
    nodeEndpoint: string
  }
}

function sortFiles(files: string[]) {
  return files.slice().sort((a, b) => a.localeCompare(b, 'en', { numeric: true }))
}

const STORAGE_BAGS_QUERY = `
query GetStorageBucketBags($storageBucket: ID!, $limit: Int!, $offset: Int!) {
  storageBags(
    where: { storageBuckets_some: { id_eq: $storageBucket } }
    orderBy: createdAt_ASC
    limit: $limit
    offset: $offset
  ) {
    id
  }
}
`

const STORAGE_BAGS_OBJECTS_QUERY = `
query GetStorageBagsObjects($storageBags: [ID!]!, $limit: Int!, $offset: Int!) {
  storageBags(
    where: { id_in: $storageBags }
    orderBy: createdAt_ASC
    limit: $limit
    offset: $offset
  ) {
    id
    objects(where: {isAccepted_eq: true, createdAt_gt: $startTimestamp}) {
      id
      isAccepted
      createdAt
    }
  }
}
`

const BUCKETS_ASSIGNED_STORAGE_BAGS = `
query GetStorageBags($storageBags: [ID!]!, $limit: Int!, $offset: Int!) {
  storageBags(
    where: { id_in: $storageBags }
    orderBy: createdAt_ASC
    limit: $limit
    offset: $offset
  ) {
    id
    storageBuckets {
      id
    }
  }
}
`

const ACTIVE_BUCKET_METADATA = `
query GetActiveStorageBucketEndpoints($storageBuckets: [ID!]!, $limit: Int!, $offset: Int!) {
  storageBuckets(
    where: { id_in: $storageBuckets }
    orderBy: createdAt_ASC
    limit: $limit
    offset: $offset
  ) {
    id
    operatorStatus {
      __typename
    }
    operatorMetadata {
      nodeEndpoint
    }
  }
}
`

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
  keyOverwrite?: string
): Promise<T[]> => {
  let hasMoreData = true
  let offset = 0
  let data: T[] = []
  const key = keyOverwrite || Object.keys(variables)[0]

  while (hasMoreData) {
    const response = await fetch('https://query.joystream.org/graphql', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: query,
        variables: { ...variables, limit: pageSize, offset: offset },
      }),
    })

    if (!response.ok) {
      const error = await response.text()
      console.log(error)
      throw new Error(`Error fetching data: ${response.statusText}`)
    }

    const jsonResponse = await response.json()

    data = data.concat(jsonResponse.data[key])
    hasMoreData = jsonResponse.data[key].length === pageSize

    offset += pageSize
  }

  return data
}

const getAllBucketObjects = async (bucketId: string, bagId: string) => {
  console.log('Getting bags...')
  const startTime = JSON.parse(await fs.readFile(LOCAL_FILES_PATH, 'utf-8'))?.startTime
  if (!startTime) {
    console.log('No start time found, please run localFiles first')
    process.exit(1)
  }
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
    const bucketBagsWithObjects = await fetchPaginatedData<BagWithObjects>(
      STORAGE_BAGS_OBJECTS_QUERY,
      { storageBags: bucketBagsIds.slice(i, i + BATCH_SIZE), startTimestamp: startTime },
      BATCH_SIZE
    )
    bucketBagsWithObjects.forEach((bag) => {
      const acceptedObjects = bag.objects
      if (acceptedObjects.length !== 0) {
        console.log('accepted:', acceptedObjects.length)
        bagObjectsMap[bag.id] = sortFiles(acceptedObjects.map((object) => object.id))
      }
    })
  }
  const totalObjectsCount = Object.values(bagObjectsMap).flat().length
  console.log(`Found ${totalObjectsCount} accepted objects`)
  await fs.writeFile(bagId !== null ? getRemoteBagPath(bagId) : REMOTE_FILES_PATH, JSON.stringify(bagObjectsMap))
}

const getLocalFiles = async (path: PathLike) => {
  console.log('Getting files...')
  const ts = new Date().getTime()
  const allFiles = await fs.readdir(path)
  const acceptedFiles: string[] = []
  const recentFiles: string[] = []
  allFiles.forEach((file) => {
    if (!isNaN(parseInt(file))) {
      return
    }
    if (ts - getFileBirthtime(file).getTime() > RECENT_FILES_THRESHOLD) {
      acceptedFiles.push(file)
    } else {
      recentFiles.push(file)
    }
  })
  console.log(`Found ${acceptedFiles.length} files. Skipping ${recentFiles.length} recent files.`)
  const sortedFiles = sortFiles(acceptedFiles)
  await fs.writeFile(
    LOCAL_FILES_PATH,
    JSON.stringify({ acceptedFiles: sortedFiles, startTime: ts - RECENT_FILES_THRESHOLD, skippedFiles: recentFiles })
  )
}

const getDifferences = async (bagId: string) => {
  const localFiles: string[] = JSON.parse(await fs.readFile(LOCAL_FILES_PATH, 'utf-8'))?.acceptedFiles || []
  const remoteFiles: { [key: string]: string[] } = JSON.parse(
    await fs.readFile(bagId != null ? getRemoteBagPath(bagId) : REMOTE_FILES_PATH, 'utf-8')
  )

  const localFilesSet = new Set(localFiles)
  const allRemoteFilesSet = new Set(Object.values(remoteFiles).flat())

  const unexpectedLocal = new Set([...localFilesSet].filter((id) => !allRemoteFilesSet.has(id)))
  console.log(`Unexpected local files: ${JSON.stringify([...unexpectedLocal])}`)

  const missingObjectsPerBag: { [bagId: string]: string[] } = {}
  Object.entries(remoteFiles).forEach(([bagId, objects]) => {
    const missingObjects = objects.filter((id) => !localFilesSet.has(id))
    if (missingObjects.length !== 0) {
      console.log(`Bag ${bagId} missing ${missingObjects.length} objects: ${JSON.stringify(missingObjects)}`)
      missingObjectsPerBag[bagId] = missingObjects
    }
  })

  const missingObjects = new Set(Object.values(missingObjectsPerBag).flat())

  console.log(`Missing ${missingObjects.size} objects`)
  console.log(`Found ${unexpectedLocal.size} unexpected local objects`)

  await fs.writeFile(
    bagId != null ? getDiffBagPath(bagId) : DIFF_PATH,
    JSON.stringify({
      unexpectedLocal: [...unexpectedLocal],
      missingObjectsPerBag: missingObjectsPerBag,
    })
  )
}

const getMissing = async (diffFilePath: string, ignoreProviders: number[]) => {
  console.log(`Checking missing objects from ${diffFilePath}...`)
  const missingObjects = JSON.parse(await fs.readFile(diffFilePath, 'utf-8')).missingObjectsPerBag

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
    const bagResult: {
      storageBag: string
      operatorsChecked: string[]
      dataObjects: string[]
      granularResults: {
        bucketId: string
        result: number | string
      }[][]
      foundObjects: number
    } = {
      storageBag: bagId,
      operatorsChecked: assignedSps,
      dataObjects: missingObjectsInBag,
      granularResults: [],
      foundObjects: 0,
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
  await fs.writeFile(CHECK_PATH, JSON.stringify(results, null, 2))
}

async function manualHeadRequest(url: string, objectId: string) {
  try {
    const res = await headRequestAsset(url, objectId)
    console.log('Res', res)
  } catch (err) {
    console.log('err', err)
  }
}

const command = process.argv[2]
const arg = process.argv[3]
const arg2 = process.argv[4]

if (command === 'localFiles') {
  if (!arg) {
    console.log('Please provide a path')
    process.exit(1)
  }
  getLocalFiles(arg)
} else if (command === 'bucketObjects') {
  if (!arg || isNaN(parseInt(arg))) {
    console.log('Please provide a bucket id')
    process.exit(1)
  }
  if (arg2 && isNaN(parseInt(arg2))) {
    console.log('If you want to get only a single bag, provide only the number from the id, dynamic:channel:XXX')
    process.exit(1)
  }
  getAllBucketObjects(arg, arg2)
} else if (command === 'diff') {
  if (arg && isNaN(parseInt(arg))) {
    console.log('If you want to diff only a single bag, provide only the number from the id, dynamic:channel:XXX')
    process.exit(1)
  }
  getDifferences(arg)
} else if (command === 'checkMissing') {
  let path = DIFF_PATH
  let providerInputs = undefined
  const ignoreProviders: number[] = []
  if (arg) {
    if (arg.includes('.json')) {
      path = arg
    } else {
      providerInputs = arg
    }
  }
  if (arg2) {
    providerInputs = arg2
  }
  if (providerInputs) {
    try {
      providerInputs.split(',').forEach((sp) => ignoreProviders.push(parseInt(sp)))
    } catch (err) {
      console.log(`Invalid input for providers, use format 1 or 0,1,4. Err: ${err}`)
      process.exit(1)
    }
  }
  getMissing(path, ignoreProviders)
} else if (command === 'head') {
  if (arg && arg2) {
    manualHeadRequest(arg, arg2)
  } else if (!arg2) {
    const providerUrl = `${arg.split('files/')[0]}files`
    const objectId = arg.split('files/')[1]
    manualHeadRequest(providerUrl, objectId)
  } else {
    console.log('Input must be head <URL/api/v1/files> <objectId>')
    process.exit(1)
  }
} else {
  console.log('Unknown command')
  process.exit(1)
}
