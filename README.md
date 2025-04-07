# MongoDB CSFLE Migration

Project time usage:

[![wakatime](https://wakatime.com/badge/user/20f31d58-e08c-46c7-9266-c37aed16eebc/project/10789f71-71a1-4a25-9034-a4ce4ef51b7e.svg)](https://wakatime.com/badge/user/20f31d58-e08c-46c7-9266-c37aed16eebc/project/10789f71-71a1-4a25-9034-a4ce4ef51b7e)

## Overview
This project contains a Java sample that demonstrates how to migrate a MongoDB database from a non-encrypted collection to an encrypted collection using Client-Side Field Level Encryption (CSFLE). The sample uses the MongoDB Java Driver and the MongoDB Encrypted Fields library.


## Application Detail

### Main Class
- App.java
  - `app.migrator.csfle.App`

## Usage

### Commands
- **`migrate`** - Migrate the data from the non-encrypted collection to the encrypted collection.
- **`generate-dekid`** - Generate a data encryption key (DEK) for the encrypted collection.

### Parameters

**Global Parameters**
- **`--config`** - Path to the configuration file (JSON format).
- **`--schema`** - Path to the schema file (JSON format).
- **`--migration-config`** - Path to the migration configuration file (JSON format).

**TLS/SSL Configuration for KMS**
- **`javax.net.ssl.keyStoreType`** - The type of the keystore (e.g., JKS, PKCS12).
- **`javax.net.ssl.keyStore`** - The path to the keystore file.
- **`javax.net.ssl.keyStorePassword`** - The password for the keystore.
- **`javax.net.ssl.trustStoreType`** - The type of the truststore (e.g., JKS, PKCS12).
- **`javax.net.ssl.trustStore`** - The path to the truststore file.
- **`javax.net.ssl.trustStorePassword`** - The password for the truststore.

**Sample TLS/SSL Configuration**
```bash
java -jar MongoDBCSFLEMigrator-1.0.1b-SNAPSHOT-jar-with-dependencies.jar \
  # SSL
  -Djavax.net.ssl.keyStoreType=<your-keystore-type> \
  -Djavax.net.ssl.keyStore=<path-to-your-keystore> \
  -Djavax.net.ssl.keyStorePassword=<your-keystore-password> \
  # Truststore
  -Djavax.net.ssl.trustStoreType=<your-truststore-type> \
  -Djavax.net.ssl.trustStore=<path-to-your-truststore> \
  -Djavax.net.ssl.trustStorePassword=<your-truststore-password> \
migrate
```


## For Development usage
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
mvn clean compile exec:java -Dexec.mainClass="app.migrator.csfle.App" \
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
---
### Export dependencies
```bash
mvn dependency:copy-dependencies -DoutputDirectory=target/lib
```
---
### Run the application with Non-Fat JAR
```bash
java -cp target/MongoDBCSFLEMigrator/MongoDBCSFLEMigrator-1.0.1b-SNAPSHOT-jar-with-dependencies.jar:target/lib app.migrator.csfle.App
```


> [!WARNING]
> ## Disclaimer
> This software is not officially supported by MongoDB, Inc. under > any commercial support subscriptions or other agreements. Use of these scripts is at your own risk.
>
> Requests for modifications may require MongoDB Professional Services and may incur additional service days.