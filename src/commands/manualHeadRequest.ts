import {headRequestAsset} from "../utils/headRequestAsset.js";

export const manualHeadRequest = async () => {
    const url = prompt('Enter request url: ')
    if (!url) {
        console.log('Url is required')
        process.exit(1)
    }
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
