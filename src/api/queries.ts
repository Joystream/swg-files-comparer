export const STORAGE_BAGS_QUERY = `
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

export const STORAGE_BAGS_OBJECTS_QUERY = `
query GetStorageBagsObjects($storageBags: [ID!]!, $limit: Int!, $offset: Int!, $startTimestamp: DateTime!) {
storageDataObjects(where: {storageBag: {id_in: $storageBags}, createdAt_lt: $startTimestamp, isAccepted_eq: true}, limit: $limit, offset: $offset) {
    id
    isAccepted
    storageBag {
      id
    }
  }
}
`

export const BUCKETS_ASSIGNED_STORAGE_BAGS = `
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

export const ACTIVE_BUCKET_METADATA = `
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
