package me.jirachai.mongodb.migrator.csfle.service;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.slf4j.Logger;
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
  private final Logger logger = org.slf4j.LoggerFactory.getLogger(MongoCSFLE.class);

  private final Configuration configuration;
  private String cryptSharedLibPath = "./lib/mongo_crypt_shared_v1-macos-arm64-enterprise-8.0.6/lib/mongo_crypt_v1.dylib";
  //
  // KMS Provider: local
  private String masterKeyFilePath;
  // KMS Provider: KMIP
  private String kmsEndpoint;

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
  private KmsProvider kmsProviderEnum = KmsProvider.LOCAL;
  //
  private Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>();
  private Map<String, Object> providerDetails = new HashMap<>();

  private HashMap<String, BsonDocument> schemaMap;

  public MongoCSFLE(String mongoUri, Configuration configuration) {
    this.mongoUri = mongoUri;
    this.configuration = configuration;

    EncryptionConfig encryption = configuration.getEncryption();
    this.kmsProvider = encryption.getKmsProvider();
    this.kmsProviderEnum = KmsProvider.fromString(this.kmsProvider);
    //
    this.keyVaultDb = encryption.getKeyVaultDb();
    this.keyVaultColl = encryption.getKeyVaultColl();
    this.keyVaultNamespace = encryption.getKeyVaultDb() + "." + encryption.getKeyVaultColl();
    this.cryptSharedLibPath = encryption.getCryptSharedLibPath();
    //
    // If KMS provider is not set, default to local
    if (this.kmsProvider == null) {
      this.kmsProvider = KmsProvider.LOCAL.getProvider();
      this.kmsProviderEnum = KmsProvider.LOCAL;
    }
    this.masterKeyFilePath = encryption.getMasterKeyFilePath();
    //
    // If KMS provider is KMIP, set the endpoint
    this.kmsEndpoint = encryption.getKmsEndpoint();
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
    this.clientEncryptionSettings =
        ClientEncryptionSettings.builder()
            .keyVaultMongoClientSettings(
                MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(this.mongoUri))
                    .build())
            .keyVaultNamespace(keyVaultNamespace)
            .kmsProviders(kmsProviders)
            .build();

    logger.info("ClientEncryptionSettings: " + clientEncryptionSettings.toString());
    this.clientEncryption = ClientEncryptions.create(clientEncryptionSettings);
  }

  //
  // Function to create the key vault collection and index
  public void createKeyVault() {
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
  public String generateDataKey() {
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

  public static enum KmsProvider {
    LOCAL("local"),
    AWS("aws"),
    AZURE("azure"),
    GCP("gcp"),
    KMIP("kmip");

    private final String provider;

    KmsProvider(String provider) {
      this.provider = provider;
    }

    public String getProvider() {
      return provider;
    }


    public static KmsProvider fromString(String provider) {
      for (KmsProvider p : KmsProvider.values()) {
        if (p.provider.equalsIgnoreCase(provider)) {
          return p;
        }
      }
      throw new IllegalArgumentException("No enum constant for provider: " + provider);
    }
  }

  private void setupKmsProviders() throws IOException, Exception {
    //
    if (this.kmsProvider == null) {
      throw new IllegalArgumentException("KMS provider is not set");
    }
    //
    //
    switch (this.kmsProviderEnum) {
      case LOCAL:
        // Set up the KMIP provider configuration
        byte[] localMasterKeyRead = readMasterKeyFile();
        this.providerDetails.put("key", localMasterKeyRead);
        break;
      // case AWS:
      //   this.providerDetails.put("accessKeyId", "your-access-key-id");
      //   this.providerDetails.put("secretAccessKey", "your-secret-access-key");
      //   break;
      // case AZURE:
      //   this.providerDetails.put("tenantId", "your-tenant-id");
      //   this.providerDetails.put("clientId", "your-client-id");
      //   this.providerDetails.put("clientSecret", "your-client-secret");
      //   break;
      // case GCP:
      //   this.providerDetails.put("projectId", "your-project-id");
      //   this.providerDetails.put("location", "global");
      //   break;
      case KMIP:
        this.providerDetails.put("endpoint", this.kmsEndpoint);
        this.kmsProviders.put(this.kmsProvider, this.providerDetails);
        break;
      default:
        throw new IllegalArgumentException("Unsupported KMS provider: " + this.kmsProvider + " or " + this.kmsProviderEnum);
    }
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
    SchemaConfiguration schemas = configuration.getSchema();
    this.schemaMap = schemas.getSchemas();
  }

  public void preConfigure() {
    createKeyVault();
    try {
      //
      // For local key testing first time only
      //
      // createKeyVault();
      // byte[] localMasterKeyWrite = generateMasterKey();
      // createMasterKeyFile(localMasterKeyWrite);
      //
      switch (this.kmsProviderEnum) {
        case LOCAL:
          // Generate a new local master key
          byte[] localMasterKeyWrite = generateMasterKey();
          createMasterKeyFile(localMasterKeyWrite);
          break;
        // case AWS:
        //   // Generate a new AWS master key
        //   break;
        // case AZURE:
        //   // Generate a new Azure master key
        //   break;
        // case GCP:
        //   // Generate a new GCP master key
        //   break;
        case KMIP:
          // String dekIdKey = generateDataKey();
          // Print the generated DEK ID
          // logger.info("Generated DEK ID: " + dekIdKey);
          break;
        default:
          break;
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public MongoCSFLE setup() {
    try {
      setupKmsProviders();
      setupClientEncryption();
      loadSchema();
      setClient();
      this.mongoClient = MongoClients.create(this.mongoClientSettings);
    } catch (Exception e) {
      e.printStackTrace();
    }

    return this;
  }
}
