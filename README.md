# SWG-files-comparer

This tool allows to check if some of the files for collosus were lost and if they can be found on other nodes.

<!-- toc -->

- [Installation](#installation)
- [Usage](#usage)
<!-- tocstop -->

# Installation

```shell
# Install dependencies

yarn install

# Build the project

yarn build

```

# Usage

1. Index local files

```sh-session
$ yarn run localFiles <pathToFilesDir>
```

This command will index the files in the directory and create a file `localFiles.json` in the root directory of the project. It will be used
for comparison later.

2. Index objects from bucket

```sh-session
$ yarn start bucketObjects <bucketId>
```

This will get all the bags from the bucket and index all the objects in them. It will create a file `remote.json` in the root directory of the project.

```sh-session
$ yarn start bucketObjects <bucketId> <bagId>
```

This will get all the objects from the bag and index them. It will create a file `remote-${bagId}.json` in the root directory of the project.

3. Compare the files

```sh-session
$ yarn start diff
```

This will compare your bags from `local.json` with the bags from `remote.json` and will output the results in `diff.json` in the root directory of the project and in the console.

```sh-session
$ yarn start diff <bagId>
```

This will compare your bags from `local.json` with the bags from `remote-${bagId}.json` and will output the results in `diff-${bagId}.json` in the root directory of the project and in the console.

4. Restoring missing files

```sh-session
$ yarn start restoreMissing <pathToDiff> <ignoreSp>
```

This will check the `diff.json` file and will show if some of the other SP has the missing files.
You can specify the custom path to diff file and include the SP that you want to ignore in the search.
e.g

```sh-session
$ yarn start restoreMissing ./jsons/diff.json 1,2,5
```

5. Manual HEAD request

```sh-session
$ yarn start head <providerUrl> <objectId>
# or
$ yarn start head <objectUrl>
```

This will execute a HEAD request to the specified url and will indicate a presence of the file.