import * as fs from 'fs/promises'
import { getFileBirthtime } from './utils/utils.js'
import psp from 'prompt-sync-plus'
import { format } from 'date-fns'

const getLocalFilePath = (ts: string) => `./results/local-${ts}.json`
const getRemoteFilePath = (ts: string) => `./results/remote-${ts}.json`
const getRemoteBagPath = (bagId: string, ts: string) => `./results/remote-${bagId}-${ts}.json`
const getDiffPath = (ts: string) => `./results/diff-${ts}.json`
const getDiffBagPath = (bagId: string, ts: string) => `./results/diff-${bagId}-${ts}.json`
const getCheckResultPath = (ts: string) => `./results/checked-${ts}.json`

const RECENT_FILES_THRESHOLD = 1000 * 60 * 20 // 20 minutes

const prompt = psp(undefined)

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

enum Commands {
    LocalFiles = 'localfiles',
    BucketObjects = 'bucketobjects',
    Diff = 'diff',
    CheckMissing = 'checkmissing',
    Head = 'head'
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

const getAllBucketObjects = async (fileTs: string) => {
  const bucketId = prompt('Enter bucket id: ')

  if (!bucketId || isNaN(parseInt(bucketId))) {
    console.log('Please provide a bucket id')
    process.exit(1)
  }
  const bagId = prompt('Enter bag id (optional):')

  if (bagId && isNaN(parseInt(bagId))) {
    console.log('If you want to get only a single bag, provide only the number from the id, dynamic:channel:XXX')
    process.exit(1)
  }

  console.log('Getting bags...')
  const startTime = JSON.parse(await fs.readFile(getLocalFilePath(fileTs), 'utf-8'))?.startTime
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
  await fs.writeFile(
    bagId !== null ? getRemoteBagPath(bagId, fileTs) : getRemoteFilePath(fileTs),
    JSON.stringify(bagObjectsMap)
  )
}

const getLocalFiles = async (fileTs: string) => {
  const dirPath = prompt('Enter path to your local files directory: ')
  if (!dirPath) {
    console.log('Path is incorrect or not provided')
    process.exit(1)
  }

  console.log('Getting files...')
  const ts = new Date().getTime()
  const allFiles = await fs.readdir(dirPath)
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
    getLocalFilePath(fileTs),
    JSON.stringify({ acceptedFiles: sortedFiles, startTime: ts - RECENT_FILES_THRESHOLD, skippedFiles: recentFiles })
  )
}

const getDifferences = async (fileTs: string) => {
  const bagId = prompt('Enter bag id (optional): ')

  if (bagId && isNaN(parseInt(bagId))) {
    console.log('If you want to diff only a single bag, provide only the number from the id, dynamic:channel:XXX')
    process.exit(1)
  }

  const localFiles: string[] = JSON.parse(await fs.readFile(getLocalFilePath(fileTs), 'utf-8'))?.acceptedFiles || []
  const remoteFiles: { [key: string]: string[] } = JSON.parse(
    await fs.readFile(bagId != null ? getRemoteBagPath(bagId, fileTs) : getRemoteFilePath(fileTs), 'utf-8')
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
    bagId != null ? getDiffBagPath(bagId, fileTs) : getDiffPath(fileTs),
    JSON.stringify({
      unexpectedLocal: [...unexpectedLocal],
      missingObjectsPerBag: missingObjectsPerBag,
    })
  )
}

const getMissing = async (fileTs: string) => {
  const customPath = prompt('Enter path to a diff file (optional):')
  const ignoreProvidersInput = prompt('Enter providers to ignore (optional) e.g "1,3,5":')
  let path = getDiffPath(fileTs)
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
  const missingObjects = JSON.parse(await fs.readFile(path, 'utf-8')).missingObjectsPerBag

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
  await fs.writeFile(getCheckResultPath(fileTs), JSON.stringify(results, null, 2))
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

const command = prompt('Enter command:\n(command list is available in readme)\n').toLowerCase()
const fileTs = format(new Date(), 'yyyy-MM-dd-HH-mm')

switch (command) {
  case Commands.LocalFiles:
    getLocalFiles(fileTs)
    break
  case Commands.BucketObjects:
    getAllBucketObjects(fileTs)
    break
  case Commands.Diff:
    getDifferences(fileTs)
    break
  case Commands.CheckMissing:
    getMissing(fileTs)
    break
  case Commands.Head:
    manualHeadRequest()
    break
  default:
    console.log('Unknown command')
    process.exit(1)
}
