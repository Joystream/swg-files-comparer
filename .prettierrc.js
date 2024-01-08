
import prettierConfig from '@joystream/prettier-config';

export default {
  ...prettierConfig,
  printWidth: 120,
  importOrder: ['^@/(.*)$', '^./', '^[./]'],
  importOrderParserPlugins: ['jsx', 'typescript'],
  importOrderSeparation: true,
  importOrderSortSpecifiers: true,
};