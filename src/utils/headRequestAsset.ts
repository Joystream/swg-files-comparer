import fetch from "node-fetch";

export const headRequestAsset = async (baseUrl: string, objectId: string) => {
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