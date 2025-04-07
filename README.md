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

## Usage

### Development
1. Clone the repository
2. Install dependencies
```bash
mvn clean install
```
3. Create configuration files
- `config.json`
- `schema.json`
- `migration-config.json`

4. Run the application (Test the migration function)
```bash
mvn clean compile exec:java -Dexec.mainClass="me.jirachai.mongodb.migrator.csfle.App" \
  # SSL
  -Djavax.net.ssl.keyStoreType=<your-keystore-type> \
  -Djavax.net.ssl.keyStore=<path-to-your-keystore> \
  -Djavax.net.ssl.keyStorePassword=<your-keystore-password> \
  # Truststore
  -Djavax.net.ssl.trustStoreType=<your-truststore-type> \
  -Djavax.net.ssl.trustStore=<path-to-your-truststore> \
  -Djavax.net.ssl.trustStorePassword=<your-truststore-password> \
  # Function
  -Dexec.args="migrate"
```