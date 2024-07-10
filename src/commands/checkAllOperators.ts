import {BagWithObjects, StorageBuckets, StorageObject} from "../types.js";
import {ALL_STORAGE_BAGS_OBJECTS_QUERY, ALL_STORAGE_OPERATORS} from "../api/queries.js";
import fs from "fs";
import {getAllLocalFilePath, getAllRemoteFilePath} from "../utils/fs.js";
import {fetchPaginatedData} from "../utils/fetchPaginatedData.js";
import {checkRemoteNode} from "./checkRemoteNode.js";
import {getDifferences} from "./getDifferences.js";
import {store} from "../store.js";

export const checkAllOperators = async () => {
    const filesPostfix = store.getState('filesPostfix')
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
