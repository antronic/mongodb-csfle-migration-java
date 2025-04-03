package me.jirachai.mongodb.migrator.csfle.config;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

@Data
public class Configuration {
  private String sourceMongoDBUri;
  private String targetMongoDBUri;
  private String[] sourceDatabases;

  private WorkerConfig worker = new WorkerConfig();
  private EncryptionConfig encryption = new EncryptionConfig();

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
  public static class EncryptionConfig {
    private String keyVaultDb = "encryption";
    private String keyVaultColl = "__keyVault";

    private String kmsProvider = "local";
    private String masterKeyFilePath;
    private String cryptSharedLibPath;
    private Map<String, Object> extraOptions = new HashMap<>();

    private String getKeyVaultNamespace() {
      return keyVaultDb + "." + keyVaultColl;
    }
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
    if (userConfig.getSourceDatabases() != null)
      defaultConfig.setSourceDatabases(userConfig.getSourceDatabases());

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
        defaultEnc.setCryptSharedLibPath(userEnc.getCryptSharedLibPath());
      if (userEnc.getExtraOptions() != null)
        defaultEnc.getExtraOptions().putAll(userEnc.getExtraOptions());
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
    if (config.getSourceDatabases() == null) {
      throw new IllegalArgumentException("sourceDatabases is required");
    }
    if (config.getEncryption().getMasterKeyFilePath() == null) {
      throw new IllegalArgumentException("encryption.masterKeyFilePath is required");
    }
    if (config.getEncryption().getCryptSharedLibPath() == null) {
      throw new IllegalArgumentException("encryption.cryptSharedLibPath is required");
    }
  }
}
