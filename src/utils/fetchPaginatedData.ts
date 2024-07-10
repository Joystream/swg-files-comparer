import fetch from "node-fetch";

export const fetchPaginatedData = async <T>(
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
        const response = await fetch(process.env.QN_BASEURL || 'https://query.joystream.org/graphql', {
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