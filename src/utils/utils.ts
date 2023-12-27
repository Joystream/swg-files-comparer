import * as fs from "fs";

export const getFileBirthtime = (filePath: string): Date => {
  const stats = fs.statSync(filePath);
  return stats.birthtime;
}