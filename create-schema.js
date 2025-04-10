db.getSiblingDB('app')
  .createCollection('users', {
    validator: {
      $jsonSchema: {
        bsonType: 'object',
        required: [
          'username',
          'password'
        ],
        encryptMetadata: {
          keyId: [UUID('d1594b63-65c2-40b0-82ca-adfa1529cc7d')],
          algorithm: 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic',
        },
        properties: {
          username: {
            bsonType: 'string'
          },
          password: {
            encrypt: {
              bsonType: 'string',
              algorithm: 'AEAD_AES_256_CBC_HMAC_SHA_512-Random'
            }
          },
          ssn: {
            encrypt: {
              bsonType: 'string',
              algorithm: 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic'
            }
          },
          detailMessage: {
            encrypt: {
              bsonType: 'string'
            }
          },
          createdAt: {
            bsonType: 'date'
          }
        }
      },
    },
    validationLevel: 'strict',
    validationAction: 'error'
  })