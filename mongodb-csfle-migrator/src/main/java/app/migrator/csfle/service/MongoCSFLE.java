package app.migrator.csfle.service;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

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
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;
import com.mongodb.connection.SslSettings;

import app.migrator.csfle.config.Configuration;
import app.migrator.csfle.config.EncryptionConfiguration;
import app.migrator.csfle.config.SchemaConfiguration;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(chain=true)
public class MongoCSFLE {
  // Logger for logging messages
  private final Logger logger = org.slf4j.LoggerFactory.getLogger(MongoCSFLE.class);
  //
  // MongoDB connection URI
  private final Configuration configuration;
  private final String mongoUri;
  private String cryptSharedLibPath = "./lib/mongo_crypt_shared_v1-macos-arm64-enterprise-8.0.6/lib/mongo_crypt_v1.dylib";
  //
  // KMS Provider: local
  private final String masterKeyFilePath;
  // KMS Provider: KMIP
  private final String kmsEndpoint;
  //
  // MongoDB connection configuration
  @Getter
  @Setter
  private MongoClientSettings.Builder mongoClientSettingsBuilder;
  //
  // MongoDB client
  private ClientEncryption clientEncryption;
  @Setter
  private ClientEncryptionSettings clientEncryptionSettings;
  private AutoEncryptionSettings autoEncryptionSettings;
  //
  // Key vault database and collection
  private final String keyVaultDb;
  private final String keyVaultColl;
  private final String keyVaultNamespace;
  //
  // KMIP provider configuration
  private String kmsProvider = "local";
  private KmsProvider kmsProviderEnum = KmsProvider.LOCAL;
  //
  private final Map<String, Map<String, Object>> kmsProviders = new HashMap<>();
  private final Map<String, Object> providerDetails = new HashMap<>();

  private HashMap<String, BsonDocument> schemaMap;
  @Setter
  private MongoClient mongoClient;

  public MongoCSFLE(String mongoUri, Configuration configuration) {
    //
    // Initialize the MongoDB CSFLE service with the provided MongoDB URI and configuration
    this.configuration = configuration;
    this.mongoUri = mongoUri;
    //
    // Set up the MongoDB connection configuration
    EncryptionConfiguration encryption = configuration.getEncryption();
    this.kmsProvider = encryption.getKmsProvider();
    this.kmsProviderEnum = KmsProvider.fromString(this.kmsProvider);
    //
    // Set up the key vault database and collection
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

  private Map<String, SSLContext> createKmipSSLContextMap() throws Exception {
    String keyStorePath = configuration.getEncryption().getKeyStorePath();
    String keyStorePassword = configuration.getEncryption().getKeyStorePassword();
    String trustStorePath = configuration.getEncryption().getTrustStorePath();
    String trustStorePassword = configuration.getEncryption().getTrustStorePassword();
    String keyStoreType = configuration.getEncryption().getKeyStoreType();
    String trustStoreType = configuration.getEncryption().getTrustStoreType();

    logger.info(
        "KeyStore Path: " + keyStorePath +
        // "\nKeyStore Password: " + keyStorePassword +
        "\nTrustStore Path: " + trustStorePath +
        "\nTrustStore Password: " + trustStorePassword +
        "\nKeyStore Type: " + keyStoreType +
        "\nTrustStore Type: " + trustStoreType
    );

    SSLContext sslContext = SSLContextFactory.create(
        keyStorePath,
        keyStorePassword,
        trustStorePath,
        trustStorePassword,
        keyStoreType,
        trustStoreType);

    SslSettings.builder()
        .enabled(true)
        .invalidHostNameAllowed(true)
        .context(sslContext)
        .build();

    // StreamFactoryFactory sff = NettyStreamFactoryFactory.builder()
    //   .sslContext(sslContext)
    //   .build();

    Map<String, SSLContext> sslContextMap = new HashMap<>();
    sslContextMap.put(
        KmsProvider.KMIP.getProvider(),
        sslContext
    );

    return sslContextMap;
  }

  private void setupClientConfiguration() throws Exception {
    // Set up the KMS providers
    Map<String, Object> extraOptions = new HashMap<>();
    extraOptions.put("cryptSharedLibPath", cryptSharedLibPath);

    this.autoEncryptionSettings =
        AutoEncryptionSettings.builder()
            /*
             * Uncomment the following line to use a custom key vault MongoClientSettings
             */
            // .keyVaultMongoClientSettings(
            //     MongoClientSettings.builder()
            //         .applyConnectionString(
            //           new ConnectionString(this.mongoUri)
            //         )
            //         .build())
            .keyVaultNamespace(keyVaultNamespace)
            .kmsProviders(kmsProviders)
            .schemaMap(schemaMap)
            .extraOptions(extraOptions)
            .kmsProviderSslContextMap(this.createKmipSSLContextMap())
            .build();

    this.mongoClientSettingsBuilder =
        MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(this.mongoUri))
            .applyToConnectionPoolSettings(
                builder -> {
                  builder.minSize(0);
                  builder.maxSize(10);
                })
            .applyToSocketSettings(
                builder -> {
                  builder.connectTimeout(10, TimeUnit.SECONDS);
                  builder.readTimeout(10, TimeUnit.SECONDS);
                })
            .autoEncryptionSettings(autoEncryptionSettings);
  }

  private void setupClientEncryption() throws Exception {
    this.clientEncryptionSettings =
        ClientEncryptionSettings.builder()
            .keyVaultMongoClientSettings(
              this.mongoClientSettingsBuilder
                .build()
              )
            .keyVaultNamespace(keyVaultNamespace)
            .kmsProviders(kmsProviders)
            .kmsProviderSslContextMap(this.createKmipSSLContextMap())
            .build();

    // logger.info("ClientEncryptionSettings: " + clientEncryptionSettings.toString());
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
    // MongoClient _mongoClient = MongoClients.create(this.mongoUri);
    MongoDatabase database = this.mongoClient.getDatabase(this.keyVaultDb);
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
          this.kmsProviders.put(this.kmsProvider, this.providerDetails);
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

  public boolean isCryptSharedLibExist() {
    try (FileInputStream fis = new FileInputStream(this.cryptSharedLibPath)) {
      return true;
    } catch (FileNotFoundException e) {
      return false;
    } catch (IOException e1) {
      logger.error("Error reading crypt shared library: " + e1.getMessage());
    }
    return false;
  }

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

    } catch (IOException e) {
      logger.error("Error generating master key: " + e.getMessage());
    }
  }

  public MongoCSFLE setup() {
    try {
      if (!isCryptSharedLibExist()) {
        logger.error("Crypt shared library not found: " + this.cryptSharedLibPath);
        throw new RuntimeException("Crypt shared library not found: " + this.cryptSharedLibPath);
      }
      // Check if the crypt shared library exists
      setupKmsProviders();
      setupClientEncryption();
      loadSchema();
      setupClientConfiguration();
    } catch (Exception e) {
      logger.error("Error setting up MongoDB CSFLE: " + e.getMessage());
    }

    return this;
  }
}
