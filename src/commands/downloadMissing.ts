import fs from "fs";
import {FILE_BASE_PATH, getDiffPath} from "../utils/fs.js";
import {BagResult} from "../types.js";
import fetch from "node-fetch";
import {store} from "../store.js";

export const downloadMissing = async () => {
    const filesPostfix = store.getState('filesPostfix')
    const diff = JSON.parse(await fs.promises.readFile(getDiffPath(filesPostfix), 'utf-8'))
    const urls = diff.map((bagResult: BagResult) => bagResult.foundObjectsUrls).flat()
    if (urls.length === 0) {
        console.log('No missing objects found')
        process.exit(1)
    }
    console.log(`Downloading ${urls.length} missing objects...`)
    urls.forEach(async (url: string) => {
        const fileName = url.split('/').slice(-1)[0]
        await fetch(url).then((res) => {
            const dest = fs.createWriteStream(`${FILE_BASE_PATH}/${fileName}`)
            res?.body?.pipe(dest)
        })
    })
}