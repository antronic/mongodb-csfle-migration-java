# MongoDB CSFLE Migration

Project time usage:

[![wakatime](https://wakatime.com/badge/user/20f31d58-e08c-46c7-9266-c37aed16eebc/project/10789f71-71a1-4a25-9034-a4ce4ef51b7e.svg)](https://wakatime.com/badge/user/20f31d58-e08c-46c7-9266-c37aed16eebc/project/10789f71-71a1-4a25-9034-a4ce4ef51b7e)

## Overview
This project contains a Java sample that demonstrates how to migrate a MongoDB database from a non-encrypted collection to an encrypted collection using Client-Side Field Level Encryption (CSFLE). The sample uses the MongoDB Java Driver and the MongoDB Encrypted Fields library.


## Configuration

### Local key
```json
...
 "encryption": {
    "kmsProvider": "local",
    "masterKeyFilePath": "./master-key.txt",
    ...
 }
...
```


### KMS
```json
...
  "encryption": {
    "kmsProvider": "kmip",
    "kmsEndpoint": "pykmip-01.local:5696",
    ...
  }
...
```