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
  private String sourceMongoDBUri;
  private String targetMongoDBUri;

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
    if (userConfig.getSourceMongoDBUri() != null)
      defaultConfig.setSourceMongoDBUri(userConfig.getSourceMongoDBUri());
    if (userConfig.getTargetMongoDBUri() != null)
      defaultConfig.setTargetMongoDBUri(userConfig.getTargetMongoDBUri());
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

  private static void validateConfiguration(Configuration config) {
    if (config.getSourceMongoDBUri() == null) {
      throw new IllegalArgumentException("sourceMongoDBUri is required");
    }
    if (config.getTargetMongoDBUri() == null) {
      throw new IllegalArgumentException("targetMongoDBUri is required");
    }
    if (config.getEncryption().getKmsProvider().equals("local") && config.getEncryption().getMasterKeyFilePath() == null) {
      throw new IllegalArgumentException("encryption.masterKeyFilePath is required");
    }
    if (config.getEncryption().getKmsProvider().equals("kmip") && config.getEncryption().getKmsEndpoint() == null) {
      throw new IllegalArgumentException("encryption.kmsEndpoint is required");
    }

    if (config.getEncryption().getCryptSharedLibPath() == null) {
      throw new IllegalArgumentException("encryption.cryptSharedLibPath is required");
    }

    if (config.getEncryption().getKeyStorePath() == null) {
      throw new IllegalArgumentException("encryption.keyStorePath is required");
    }
    if (config.getEncryption().getKeyStorePassword() == null) {
      throw new IllegalArgumentException("encryption.keyStorePassword is required");
    }
    if (config.getEncryption().getKeyStoreType() == null) {
      throw new IllegalArgumentException("encryption.keyStoreType is required");
    }
    if (config.getEncryption().getTrustStorePath() == null) {
      throw new IllegalArgumentException("encryption.trustStorePath is required");
    }
    if (config.getEncryption().getTrustStorePassword() == null) {
      throw new IllegalArgumentException("encryption.trustStorePassword is required");
    }
    if (config.getEncryption().getTrustStoreType() == null) {
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
