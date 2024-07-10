import fs from 'fs'
import { sortFiles } from '../utils/utils.js'
import { setLocalFilePath } from '../utils/fs.js'
import { store } from '../store.js'
import { prompt } from '../utils/prompt.js'

export const getLocalFiles = async () => {
  const nextInc = store.getState('nextInc')
  const dirPath = prompt('Enter path to your local files directory: ')
  if (!dirPath) {
    console.log('Path is incorrect or not provided')
    process.exit(1)
  }

  console.log('Getting files...')
  const ts = new Date().getTime()
  const allFiles = await fs.promises.readdir(dirPath)
  const acceptedFiles = allFiles.filter((file) => !isNaN(parseInt(file)))
  console.log(`Found ${acceptedFiles.length} files.`)
  const sortedFiles = sortFiles(acceptedFiles)
  await fs.promises.writeFile(setLocalFilePath(nextInc), JSON.stringify(sortedFiles))
}
