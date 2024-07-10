import {getCheckResultPath, getDiffPath} from "../utils/fs.js";
import fs from "fs";
import {ACTIVE_BUCKET_METADATA, BUCKETS_ASSIGNED_STORAGE_BAGS} from "../api/queries.js";
import {ActiveBucketMetadataResponse, BagResult} from "../types.js";
import {fetchPaginatedData} from "../utils/fetchPaginatedData.js";
import {headRequestAsset} from "../utils/headRequestAsset.js";
import {store} from "../store.js";
import {prompt} from "../utils/prompt.js";

export const getMissing = async () => {
    const filesPostfix = store.getState('filesPostfix')
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