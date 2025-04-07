# DRAFT

## Configuration

Configure your KMS Configuration

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
