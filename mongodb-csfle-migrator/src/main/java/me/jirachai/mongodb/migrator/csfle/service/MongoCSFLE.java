package me.jirachai.mongodb.migrator.csfle.service;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;
import lombok.Getter;
import me.jirachai.mongodb.migrator.csfle.config.Configuration;
import me.jirachai.mongodb.migrator.csfle.config.SchemaConfiguration;
import me.jirachai.mongodb.migrator.csfle.config.Configuration.EncryptionConfig;

public class MongoCSFLE {
  private final Configuration configuration;
  private String masterKeyFilePath;
  private String cryptSharedLibPath = "./lib/mongo_crypt_shared_v1-macos-arm64-enterprise-8.0.6/lib/mongo_crypt_v1.dylib";

  private String mongoUri;
  @Getter
  private MongoClient mongoClient;
  private MongoClientSettings mongoClientSettings;

  private ClientEncryption clientEncryption;
  private ClientEncryptionSettings clientEncryptionSettings;
  private AutoEncryptionSettings autoEncryptionSettings;

  private String keyVaultDb;
  private String keyVaultColl;
  private String keyVaultNamespace;

  //
  // KMIP provider configuration
  private String kmsProvider = "local";
  private Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>();
  private Map<String, Object> providerDetails = new HashMap<>();

  private HashMap<String, BsonDocument> schemaMap;

  public MongoCSFLE(String mongoUri, Configuration configuration) {
    this.mongoUri = mongoUri;
    this.configuration = configuration;

    EncryptionConfig encryption = configuration.getEncryption();
    this.kmsProvider = encryption.getKmsProvider();
    this.keyVaultDb = encryption.getKeyVaultDb();
    this.keyVaultColl = encryption.getKeyVaultColl();
    this.keyVaultNamespace = encryption.getKeyVaultDb() + "." + encryption.getKeyVaultColl();
    this.cryptSharedLibPath = encryption.getCryptSharedLibPath();
    this.masterKeyFilePath = encryption.getMasterKeyFilePath();
  }

  private void setClient() {
    Map<String, Object> extraOptions = new HashMap<String, Object>();
    extraOptions.put("cryptSharedLibPath", cryptSharedLibPath);

    this.autoEncryptionSettings =
        AutoEncryptionSettings.builder()
            .keyVaultMongoClientSettings(
                MongoClientSettings.builder()
                    .applyConnectionString(
                      new ConnectionString(this.mongoUri)
                    )
                    .build())
            .keyVaultNamespace(keyVaultNamespace)
            .kmsProviders(kmsProviders)
            .schemaMap(schemaMap)
            .extraOptions(extraOptions)
            .build();

    this.mongoClientSettings =
        MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(this.mongoUri))
            .autoEncryptionSettings(autoEncryptionSettings)
            .build();
  }

  private void setupClientEncryption() {
    EncryptionConfig encryption = configuration.getEncryption();
    this.masterKeyFilePath = encryption.getMasterKeyFilePath();
    this.cryptSharedLibPath = encryption.getCryptSharedLibPath();

    this.clientEncryptionSettings =
        ClientEncryptionSettings.builder()
            .keyVaultMongoClientSettings(
                MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(this.mongoUri))
                    .build())
            .keyVaultNamespace(keyVaultNamespace)
            .kmsProviders(kmsProviders)
            .build();

    this.clientEncryption = ClientEncryptions.create(clientEncryptionSettings);
  }

  //
  // Function to create the key vault collection and index
  private void createKeyVault() {
    //
    // Determine the index options for the key vault collection
    IndexOptions indexOptions = new IndexOptions()
      .partialFilterExpression(
        new BsonDocument(
          "keyAltName", new BsonDocument("$exists", new BsonBoolean(true))
        )
      ).unique(true);
    //
    // Create the key vault collection
    MongoClient _mongoClient = MongoClients.create(this.mongoUri);
    MongoDatabase database = _mongoClient.getDatabase(this.keyVaultDb);
    MongoCollection<Document> collection = database.getCollection(keyVaultColl);
    // Create the key vault index
    collection.createIndex(
      new BsonDocument(
        "keyAltName",
        new BsonInt32(1)
      ),
      indexOptions
    );
  }

  //
  // Function to generate a new data encryption key (DEK) for the KMIP provider
  private String generateDataKey() {
    BsonBinary dataKeyId = this.clientEncryption
      .createDataKey(
        kmsProvider,
        new DataKeyOptions().masterKey(new BsonDocument())
      );
    String base64DataKeyId = Base64
      .getEncoder()
      .encodeToString(dataKeyId.getData());
    return base64DataKeyId;
  }
  //
  // Function to generate a new data encryption key (DEK) for the KMIP provider
  private byte[] generateMasterKey() {
    byte[] localMasterKeyWrite = new byte[96];

    new SecureRandom().nextBytes(localMasterKeyWrite);

    return localMasterKeyWrite;
  }

  public void createMasterKeyFile(byte[] localMasterKeyWrite) throws FileNotFoundException, IOException {
    try (FileOutputStream stream = new FileOutputStream(masterKeyFilePath)) {
      stream.write(localMasterKeyWrite);
    }
  }

  public byte[] readMasterKeyFile() throws IOException, Exception {
    byte[] localMasterKeyRead = new byte[96];

    try (FileInputStream fis = new FileInputStream(masterKeyFilePath)) {
      if (fis.read(localMasterKeyRead) < 96)
        throw new Exception("Expected to read 96 bytes from file");
    }

    return localMasterKeyRead;
  }

  private void setupKmsProviders() throws IOException, Exception {
    // Set up the KMIP provider configuration
    byte[] localMasterKeyRead = readMasterKeyFile();

    this.providerDetails.put("key", localMasterKeyRead);
    this.kmsProviders.put(this.kmsProvider, this.providerDetails);
    //
    // TODO: For KMS
    // Set up the KMIP provider configuration
    // this.providerDetails.put("endpoint", this.kmsEndpoint);
    // this.kmsProviders.put(this.kmsProvider, this.providerDetails);
  }

  // private void loadSchema(String namespace) {
  //   SchemaConfiguration schemaConfig = configuration.getSchema();
  //   if (schemaConfig != null) {
  //     Document schema = schemaConfig.getSchemaAsDocument(namespace);
  //     if (schema != null) {
  //       this.schemaMap = schema;
  //     } else {
  //       throw new IllegalArgumentException("Schema is null");
  //     }
  //   }
  // }

  public void loadSchema() {

    // String dekId = this.generateDataKey();
    // System.out.println("Generated DEK ID: " + dekId);

    // // this.schemaMap = schemas.getSchemas();
    // Document jsonSchema = new Document()
    // .append("bsonType", "object")
    // .append("encryptMetadata",
    //     new Document()
    //       .append("keyId",
    //         (new ArrayList<>(
    //           Arrays.asList(
    //             new Document()
    //               .append("$binary",
    //                 new Document()
    //                   .append("base64", dekId)
    //                   .append("subType", "04"))
    //           )
    //         ))
    //       )
    //     )
    // .append("properties", new Document()
    //   .append("secret_detail", new Document()
    //     .append("encrypt", new Document()
    //       .append("bsonType", "string")
    //       .append("algorithm", "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic")
    //     )
    //   )
    // );

    // HashMap<String, BsonDocument> schemaMap = new HashMap<String, BsonDocument>();
    // schemaMap.put("app.games", BsonDocument.parse(jsonSchema.toJson()));
    // this.schemaMap = schemaMap;

    SchemaConfiguration schemas = configuration.getSchema();
    this.schemaMap = schemas.getSchemas();
  }

  public void setup() {
    try {
      //
      // For local key testing first time only
      //
      // createKeyVault();
      // byte[] localMasterKeyWrite = generateMasterKey();
      // createMasterKeyFile(localMasterKeyWrite);


      System.out.println();
      System.out.println();
      System.out.println("URI" + this.mongoUri);
      System.out.println("Key Vault: " + this.keyVaultNamespace);
      System.out.println("Schema: " + this.schemaMap);
      System.out.println();
      System.out.println();

      setupKmsProviders();
      setupClientEncryption();
      loadSchema();
      setClient();
      this.mongoClient = MongoClients.create(this.mongoClientSettings);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
