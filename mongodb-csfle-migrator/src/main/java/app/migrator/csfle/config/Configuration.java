package app.migrator.csfle.config;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.slf4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Data;

@Data
public class Configuration {
  private static final Logger logger =
      org.slf4j.LoggerFactory.getLogger(Configuration.class);
  //
  // MongoDB connection configurations
  private MongoDBConnectionConfiguration sourceMongoDB = new MongoDBConnectionConfiguration();
  private MongoDBConnectionConfiguration targetMongoDB = new MongoDBConnectionConfiguration();
  //
  // Encryption configuration
  private WorkerConfig worker = new WorkerConfig();
  private EncryptionConfiguration encryption = new EncryptionConfiguration();

  private SchemaConfiguration schema;
  private String schemaFilePath = "schema.json";
  // private String collectionPrefix = "migrated_";

  private MigrationConfiguration migrationConfig;
  private String migrationConfigFilePath = "migration-config.json";

  private ValidationConfiguration validationConfig;
  private String validationConfigFilePath = "validation-config.json";

  @Data
  public static class WorkerConfig {
    private int maxThreads = 10;
    private int maxQueueSize = 1000;
    private int maxBatchSize = 100;
    private int maxBatchWaitTime = Integer.MAX_VALUE; // in millisecondsprivate int maxBatchSize = 100;
    private int retryDelay = 1000; // in milliseconds
    private boolean enableLogging = true;
    //
    // Read configuration
    // Type of read operation (cursor or skip)
    private String readOperationType = "cursor";

    public static void validate(WorkerConfig config) {
      // Read operation type, can be "cursor" or "skip"
      if (!Arrays.asList("cursor", "skip").contains(config.getReadOperationType())) {
        throw new IllegalArgumentException("Invalid readOperationType: " + config.getReadOperationType());
      }
      // if (config.getMaxThreads() <= 0) {
      //   throw new IllegalArgumentException("Invalid maxThreads: " + config.getMaxThreads());
      // }
      // if (config.getMaxQueueSize() <= 0) {
      //   throw new IllegalArgumentException("Invalid maxQueueSize: " + config.getMaxQueueSize());
      // }
      // if (config.getMaxBatchSize() <= 0) {
      //   throw new IllegalArgumentException("Invalid maxBatchSize: " + config.getMaxBatchSize());
      // }
      // if (config.getMaxBatchWaitTime() < 0) {
      //   throw new IllegalArgumentException("Invalid maxBatchWaitTime: " + config.getMaxBatchWaitTime());
      // }
      // if (config.getRetryDelay() < 0) {
      //   throw new IllegalArgumentException("Invalid retryDelay: " + config.getRetryDelay());
      // }
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
        Configuration userConfig = mapper.readValue(
          new File(configPath),
          Configuration.class
          );
        mergeConfigurations(defaultConfig, userConfig);
      }

      validateConfiguration(defaultConfig);
      return defaultConfig;

    } catch (IOException e) {
      throw new RuntimeException("Failed to load configuration", e);
    }
  }

  private static void mergeConfigurations(Configuration defaultConfig, Configuration userConfig) {
    // Merge source and target MongoDB configurations
    MongoDBConnectionConfiguration sourceConfig = userConfig.getSourceMongoDB();
    MongoDBConnectionConfiguration targetConfig = userConfig.getTargetMongoDB();

    defaultConfig.setSourceMongoDB(sourceConfig);
    defaultConfig.setTargetMongoDB(targetConfig);


    // Merge encryption config
    if (userConfig.getEncryption() != null) {
      // EncryptionConfiguration defaultEnc = defaultConfig.getEncryption();
      EncryptionConfiguration userEnc = userConfig.getEncryption();

      defaultConfig.setEncryption(userEnc);
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
      defaultWorker.setReadOperationType(userWorker.getReadOperationType());
    }
  }


  private static void validateConfiguration(Configuration config) {
    // Validate MongoDB connection configurations
    MongoDBConnectionConfiguration.validate(config.getSourceMongoDB(), "sourceMongoDB");
    MongoDBConnectionConfiguration.validate(config.getTargetMongoDB(), "targetMongoDB");

    // Validate encryption config
    EncryptionConfiguration.validate(config.getEncryption());

    // Validate worker config
    WorkerConfig.validate(config.getWorker());

    // Validate other configurations
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
    } catch (IOException e) {
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
    } catch (IOException e) {
      throw new RuntimeException("Failed to load configuration", e);
    }
  }

  public Configuration loadValidationTarget() {
    return loadValidationTarget(this.validationConfigFilePath);
  }

  public Configuration loadValidationTarget(String nsPath) {
    ObjectMapper mapper =
        new ObjectMapper()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    try {
      // If config file provided, merge with defaults
      if (nsPath != null) {
        ValidationConfiguration userValidation =
          mapper.readValue(
            new File(nsPath),
            ValidationConfiguration.class
          );

        this.validationConfig = userValidation;
      }
      return this;
    } catch (IOException e) {
      throw new RuntimeException("Failed to load configuration", e);
    }
  }
}
