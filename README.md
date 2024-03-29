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

Run the program using yarn start. You will see the command seleciton menu.

1. Index local files

```sh-session
 localFiles
 @param <pathToDirectory>
```

This command will index the files in the directory and create a file `localFiles.json` in the root directory of the project. It will be used
for comparison later.

2. Index objects from bucket

```sh-session
 bucketObjects
 @param <bucketId>
 @param <optional> <bagId>
```

This will get all the bags from the bucket and index all the objects in them. It will create a file `remote.json` in the root directory of the project.

With provided `bagId` it will get all the objects from the bag and index them. It will create a file `remote-${bagId}.json` in the root directory of the project.

3. Compare the files

```sh-session
diff
@param <optional><bagId>
```

This will compare your bags from `local.json` with the bags from `remote.json` and will output the results in `diff.json` in the root directory of the project and in the console.

With provided `bagId` it will compare your bags from `local.json` with the bags from `remote-${bagId}.json` and will output the results in `diff-${bagId}.json` in the root directory of the project and in the console.

4. Check missing files

```sh-session
checkMissing
@param <pathToDiff>
@param <optional> <ignoreSp>
```

This will check the `diff.json` file and will show if some of the other SP has the missing files.
You can specify the custom path to diff file and include the SP that you want to ignore in the search.
e.g

5. Download missing files

```sh-session
downloadMissing
```

This should be used after the checkMissing command. It will download the missing files from the remote node containing missing files.

```sh-session
$ yarn start restoreMissing ./jsons/diff.json 1,2,5
```

6. Manual HEAD request

```sh-session
 head <providerUrl>
 @param <providerUrl>

```

This will execute a HEAD request to the specified url and will indicate a presence of the file.

7. Check Remote node

   ```sh-session
   remotenode
   @param <providerUrl>
   ```

This will check the remote node and will return the same output as the localFiles command.

8. Check single operator

   ```sh-session
   checknode
   @param <providerUrl>
   ```

This is a combination of remoteNode, bucketObjects and diff commands for a single operator. It will check the operator, check all files for its bags in QN and will generate a diff.

8. Check all operators

   ```sh-session
   checkalloperators
   ```

This is a combination of remoteNode, bucketObjects and diff commands. It will check all the operators, check all files in QN and will generate a diff.
