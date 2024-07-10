import { BagWithObjects, StorageBucketWithBags } from '../types.js'
import { STORAGE_BAGS_OBJECTS_QUERY, STORAGE_BAGS_QUERY } from '../api/queries.js'
import fs from 'fs'
import { getRemoteBagPath, getRemoteFilePath } from '../utils/fs.js'
import {fetchPaginatedData} from "../utils/fetchPaginatedData.js";
import {store} from "../store.js";
import {RECENT_FILES_THRESHOLD} from "../utils/consts.js";
import {prompt} from "../utils/prompt.js";


export const getBucketObjects = async (id?: string) => {
  const filesPostfix = store.getState('filesPostfix')
  const bucketId = id ? id : prompt('Enter bucket id: ')

  if (!bucketId || isNaN(parseInt(bucketId))) {
    console.log('Please provide a bucket id')
    process.exit(1)
  }
  const bagId = prompt('Enter bag id (optional):')
  const timeRange = prompt('Enter start and end time (optional, format: startTimestamp-endTimestamp):')

  let startTime, endTime
  if (timeRange) {
    store.setState('customTimestamp', true)
    const timestamps = timeRange.split('-')
    startTime = timestamps[0] ? Number(timestamps[0]) : undefined
    endTime = timestamps[1] ? Number(timestamps[1]) : undefined
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
