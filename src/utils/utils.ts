import * as fs from 'fs'

export const getFileBirthtime = (filePath: string): Date => {
  const stats = fs.statSync(filePath)
  return stats.birthtime
}

export const sortFiles = (files: string[]) => files.slice().sort((a, b) => a.localeCompare(b, 'en', { numeric: true }))
