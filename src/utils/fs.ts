import fs from 'fs/promises'

export const FILE_BASE_PATH = './results/'
export const getFilePath = (fileName: string, inc: string, nodeName?: string) =>
  `${FILE_BASE_PATH}${fileName}${nodeName ? `-${nodeName}` : ''}-${inc}.json`

export const getFilePathWithPostfix = (fileName: string, postfix: string) =>
  `${FILE_BASE_PATH}${fileName}${postfix ? `-${postfix}` : '.json'}`

export const getIncValue = async () => {
  const allFiles = await fs.readdir(FILE_BASE_PATH)
  const allLocalFiles = allFiles.filter((name) => name.startsWith('local'))
  if (allLocalFiles.length === 0) {
    console.log('length: 0')
    return '0001'
  }
  console.log('length:', allLocalFiles.length)
  const incValues = allLocalFiles.map((name) => name.split('-').slice(-1)[0])
  const maxInc = incValues.reduce((prev, curr) => (prev > curr ? prev : curr))
  return ('0000' + (parseInt(maxInc) + 1)).slice(-4)
}

export const getLastLocalFileName = async () => {
  const allFiles = await fs.readdir(FILE_BASE_PATH)
  const allLocalFiles = allFiles.filter((name) => name.startsWith('local'))
  if (allLocalFiles.length === 0) {
    return ''
  }
  const incValues = allLocalFiles.map((name) => name.split('-').slice(-1)[0])
  const maxInc = incValues.reduce((prev, curr) => (prev > curr ? prev : curr))
  return allLocalFiles.find((name) => name.includes(maxInc))
}

export const getFilePostfix = async () => {
  const lastLocalFileName = await getLastLocalFileName()
  if (!lastLocalFileName) {
    return ''
  }
  return lastLocalFileName.split('-').slice(1).join('-')
}

export const setLocalFilePath = (inc: string, nodeName?: string) => getFilePath('local', inc, nodeName)
export const getLocalFilePath = (postfix: string) => getFilePathWithPostfix('local', postfix)
export const getRemoteFilePath = (postfix: string) => getFilePathWithPostfix('remote', postfix)
export const getRemoteBagPath = (bagId: string, postfix: string) =>
  getFilePathWithPostfix(`remote${bagId ? `-${bagId}` : ''}`, postfix)
export const getDiffPath = (postfix: string) => getFilePathWithPostfix('diff', postfix)
export const getDiffBagPath = (bagId: string, postfix: string) =>
  getFilePathWithPostfix(`diff${bagId ? `-${bagId}` : ''}`, postfix)
export const getCheckResultPath = (postfix: string) => getFilePathWithPostfix('checked', postfix)
