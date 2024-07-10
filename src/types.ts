export type StorageObject = {
    id: string
    isAccepted: boolean
    createdAt: string
}

export type BagWithObjects = {
    id: string
    objects: StorageObject[]
}

export type StorageDataObject = {
    id: string
    isAccepted: boolean
    storageBag: {
        id: string
    }
}

export type StorageBucketWithBags = {
    id: string
    storageBags: {
        id: string
    }[]
}

export type ActiveBucketMetadataResponse = {
    id: string
    operatorStatus: {
        __typename: string
    }
    operatorMetadata: {
        nodeEndpoint: string
    }
}

export type BagResult = {
    storageBag: string
    operatorsChecked: string[]
    dataObjects: string[]
    granularResults: {
        bucketId: string
        result: number | string
    }[][]
    foundObjects: number
    foundObjectsUrls: string[]
}

export type StorageBuckets = {
   id: string,
   operatorMetadata: {
         nodeEndpoint: string
    id: string,
       storagebucketoperatorMetadata: {
          id: string
       }[]
   }
}

export enum Commands {
    LocalFiles = 'localfiles',
    RemoteNode = 'remotenode',
    BucketObjects = 'bucketobjects',
    Diff = 'diff',
    CheckMissing = 'checkmissing',
    Head = 'head',
    CheckNode = 'checknode',
    CheckAllOperators = 'checkalloperators',
    DownloadMissing = 'downloadmissing',
    Exit = 'exit',
}