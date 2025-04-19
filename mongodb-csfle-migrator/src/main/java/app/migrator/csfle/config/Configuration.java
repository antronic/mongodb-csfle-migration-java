package app.migrator.csfle.config;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Data;

@Data
public class Configuration {
  // private String sourceMongoDBUri;
  // private String targetMongoDBUri;
  private MongoDBConnectionConfig sourceMongoDB = new MongoDBConnectionConfig();
  private MongoDBConnectionConfig targetMongoDB = new MongoDBConnectionConfig();

  private WorkerConfig worker = new WorkerConfig();
  private EncryptionConfig encryption = new EncryptionConfig();

  private SchemaConfiguration schema;
  private String schemaFilePath = "schema.json";
  // private String collectionPrefix = "migrated_";

  private MigrationConfiguration migrationConfig;
  private String migrationConfigFilePath = "migration-config.json";

  @Data
  public static class WorkerConfig {
    private int maxThreads = 10;
    private int maxQueueSize = 1000;
    private int maxBatchSize = 100;
    private int maxBatchWaitTime = 1000; // in millisecondsprivate int maxBatchSize = 100;
    private int retryDelay = 1000; // in milliseconds
    private boolean enableLogging = true;
  }

  @Data
  public class EncryptionConfig {
    private String keyVaultDb = "encryption";
    private String keyVaultColl = "__keyVault";

    private String kmsProvider = "local";
    private String cryptSharedLibPath;
    private Map<String, Object> extraOptions = new HashMap<>();
    //
    // KMIP provider configuration
    // Provider: Local
    private String masterKeyFilePath;
    // Provider: KMIP
    private String kmsEndpoint;

    // private String getKeyVaultNamespace() {
    //   return keyVaultDb + "." + keyVaultColl;
    // }

    private String keyStorePath;
    private String keyStorePassword;
    private String trustStorePath;
    private String trustStorePassword;
    private String keyStoreType;
    private String trustStoreType;
  }

  @Data
  public class MongoDBConnectionConfig {
    private String uri;
    private boolean tls;
    private String authMechanism = "SCRAM-SHA-256";
    private String authSource = "admin";
    private String tlsCAFile;
    private String tlsCertificateKeyFile;
    private String tlsCertificateKeyPassword;
    // private String tlsInsecure;

    private String username;
    private String password;
  }

  public static Configuration load(String configPath) {
    ObjectMapper mapper =
        new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    try {
      // Load default config first
      Configuration defaultConfig = new Configuration();

      // If config file provided, merge with defaults
      if (configPath != null) {
        Configuration userConfig = mapper.readValue(new File(configPath), Configuration.class);
        mergeConfigurations(defaultConfig, userConfig);
      }

      validateConfiguration(defaultConfig);
      return defaultConfig;

    } catch (Exception e) {
      throw new RuntimeException("Failed to load configuration", e);
    }
  }

  private static void mergeConfigurations(Configuration defaultConfig, Configuration userConfig) {
    // TODO: Merge MongoDB connection configs
    // if (userConfig.getSourceMongoDBUri() != null)
    //   defaultConfig.setSourceMongoDBUri(userConfig.getSourceMongoDBUri());
    // if (userConfig.getTargetMongoDBUri() != null)


    //   defaultConfig.setTargetMongoDBUri(userConfig.getTargetMongoDBUri());
    // if (userConfig.getCollectionPrefix() != null)
    //   defaultConfig.setCollectionPrefix(userConfig.getCollectionPrefix());

    // Merge encryption config
    if (userConfig.getEncryption() != null) {
      EncryptionConfig defaultEnc = defaultConfig.getEncryption();
      EncryptionConfig userEnc = userConfig.getEncryption();

      // if (userEnc.getKeyVaultNamespace() != null)
      //   defaultEnc.setKeyVaultNamespace(userEnc.getKeyVaultNamespace());
      if (userEnc.getKeyVaultDb() != null)
        defaultEnc.setKeyVaultDb(userEnc.getKeyVaultDb());
      if (userEnc.getKeyVaultColl() != null)
        defaultEnc.setKeyVaultColl(userEnc.getKeyVaultColl());
      if (userEnc.getKmsProvider() != null)
        defaultEnc.setKmsProvider(userEnc.getKmsProvider());
      if (userEnc.getMasterKeyFilePath() != null)
        defaultEnc.setMasterKeyFilePath(userEnc.getMasterKeyFilePath());
      if (userEnc.getCryptSharedLibPath() != null)
      if (userEnc.getKmsEndpoint() != null)
        defaultEnc.setKmsEndpoint(userEnc.getKmsEndpoint());
        defaultEnc.setCryptSharedLibPath(userEnc.getCryptSharedLibPath());
      if (userEnc.getExtraOptions() != null)
        defaultEnc.getExtraOptions().putAll(userEnc.getExtraOptions());
      if (userEnc.getKeyStorePath() != null)
        defaultEnc.setKeyStorePath(userEnc.getKeyStorePath());
      if (userEnc.getKeyStorePassword() != null)
        defaultEnc.setKeyStorePassword(userEnc.getKeyStorePassword());
      if (userEnc.getKeyStoreType() != null)
        defaultEnc.setKeyStoreType(userEnc.getKeyStoreType());
      if (userEnc.getTrustStorePath() != null)
        defaultEnc.setTrustStorePath(userEnc.getTrustStorePath());
      if (userEnc.getTrustStorePassword() != null)
        defaultEnc.setTrustStorePassword(userEnc.getTrustStorePassword());
      if (userEnc.getTrustStoreType() != null)
        defaultEnc.setTrustStoreType(userEnc.getTrustStoreType());
    }

    // Merge worker config
    if (userConfig.getWorker() != null) {
      WorkerConfig defaultWorker = defaultConfig.getWorker();
      WorkerConfig userWorker = userConfig.getWorker();

      defaultWorker.setMaxThreads(userWorker.getMaxThreads());
      defaultWorker.setMaxQueueSize(userWorker.getMaxQueueSize());
      defaultWorker.setMaxBatchSize(userWorker.getMaxBatchSize());
      defaultWorker.setMaxBatchWaitTime(userWorker.getMaxBatchWaitTime());
      defaultWorker.setRetryDelay(userWorker.getRetryDelay());
      defaultWorker.setEnableLogging(userWorker.isEnableLogging());
    }
  }

  private static void validateMongoDBConnectionConfig(MongoDBConnectionConfig config, String label) {
    if (config.getUri() == null) {
      throw new IllegalArgumentException(label + ".uri is required");
    }
    if (config.getAuthMechanism() == null) {
      throw new IllegalArgumentException(label + ".authMechanism is required");
    }
    switch (config.getAuthMechanism()) {
      case "SCRAM-SHA-1":
      case "SCRAM-SHA-256":
        if (config.getUsername() == null) {
          throw new IllegalArgumentException(label + ".username is required");
        }
        if (config.getPassword() == null) {
          throw new IllegalArgumentException(label + ".password is required");
        }
        break;
      case "MONGODB-X509":
        if (config.getTlsCAFile() == null) {
          throw new IllegalArgumentException(label + ".tlsCAFile is required");
        }
        if (config.getTlsCertificateKeyFile() == null) {
          throw new IllegalArgumentException(label + ".tlsCertificateKeyFile is required");
        }
        if (config.getTlsCertificateKeyPassword() == null) {
          throw new IllegalArgumentException(label + ".tlsCertificateKeyPassword is required");
        }
        break;
      case "NONE":
        // No authentication required
        break;
      default:
        throw new IllegalArgumentException(
            label + ".authMechanism must be one of SCRAM-SHA-1, SCRAM-SHA-256, MONGODB-X509, or NONE");
    }
  }


  private static void validateConfiguration(Configuration config) {
    validateMongoDBConnectionConfig(config.getSourceMongoDB(), "sourceMongoDB");
    validateMongoDBConnectionConfig(config.getTargetMongoDB(), "targetMongoDB");

    EncryptionConfig enc = config.getEncryption();

    if (enc.getKmsProvider().equals("local") && enc.getMasterKeyFilePath() == null) {
      throw new IllegalArgumentException("encryption.masterKeyFilePath is required");
    }
    if (enc.getKmsProvider().equals("kmip") && enc.getKmsEndpoint() == null) {
      throw new IllegalArgumentException("encryption.kmsEndpoint is required");
    }

    // Validate cryptSharedLibPath
    if (enc.getCryptSharedLibPath() == null) {
      throw new IllegalArgumentException("encryption.cryptSharedLibPath is required");
    }

    // Validate keyStore and trustStore
    if (enc.getKeyStorePath() == null) {
      throw new IllegalArgumentException("encryption.keyStorePath is required");
    }
    if (enc.getKeyStorePassword() == null) {
      throw new IllegalArgumentException("encryption.keyStorePassword is required");
    }
    if (enc.getKeyStoreType() == null) {
      throw new IllegalArgumentException("encryption.keyStoreType is required");
    }
    if (enc.getTrustStorePath() == null) {
      throw new IllegalArgumentException("encryption.trustStorePath is required");
    }
    if (enc.getTrustStorePassword() == null) {
      throw new IllegalArgumentException("encryption.trustStorePassword is required");
    }
    if (enc.getTrustStoreType() == null) {
      throw new IllegalArgumentException("encryption.trustStoreType is required");
    }
  }

  public Configuration loadSchema() {
    return loadSchema(this.schemaFilePath);
  }

  public Configuration loadSchema(String schemaPath) {
    ObjectMapper mapper =
        new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    try {
      // If config file provided, merge with defaults
      if (schemaPath != null) {
        Map<String, Object> schemaJson = mapper.readValue(
          new File(schemaPath),
          new TypeReference<Map<String, Object>>() {}
        );

        SchemaConfiguration schemaConfig = new SchemaConfiguration();
        schemaConfig.setSchemasObject(schemaJson);

        this.schema = schemaConfig;
      }

      return this;
    } catch (Exception e) {
      throw new RuntimeException("Failed to load configuration", e);
    }
  }

  public static String getSchemaForCollection(String collectionName) {
    // Implement the logic to get the schema for a specific collection
    // This could involve using the MongoDB Java driver to query the collection.
    return null;
  }

  public Configuration loadMigrateTarget() {
    return loadMigrateTarget(this.migrationConfigFilePath);
  }

  public Configuration loadMigrateTarget(String nsPath) {
    ObjectMapper mapper =
        new ObjectMapper()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    try {
      // If config file provided, merge with defaults
      if (nsPath != null) {
        MigrationConfiguration userMigrateTarget =
          mapper.readValue(
            new File(nsPath),
            MigrationConfiguration.class
          );

        this.migrationConfig = userMigrateTarget;
      }
      return this;
    } catch (Exception e) {
      throw new RuntimeException("Failed to load configuration", e);
    }
  }
}
