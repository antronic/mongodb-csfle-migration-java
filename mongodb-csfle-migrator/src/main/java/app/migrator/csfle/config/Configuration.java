package app.migrator.csfle.config;

import java.io.File;
import java.io.IOException;
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
  // App configuration
  private AppConfiguration app = new AppConfiguration();
  // Encryption configuration
  private EncryptionConfiguration encryption = new EncryptionConfiguration();
  // Worker configuration
  private WorkerConfiguration worker = new WorkerConfiguration();

  private SchemaConfiguration schema;
  private String schemaFilePath = "schema.json";
  // private String collectionPrefix = "migrated_";

  private MigrationConfiguration migrationConfig;
  private String migrationConfigFilePath = "migration-config.json";

  private ValidationConfiguration validationConfig;
  private String validationConfigFilePath = "validation-config.json";

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

    // Merge App config
    if (userConfig.getApp() != null) {
      AppConfiguration userApp = userConfig.getApp();
      defaultConfig.setApp(userApp);
    }

    // Merge encryption config
    if (userConfig.getEncryption() != null) {
      // EncryptionConfiguration defaultEnc = defaultConfig.getEncryption();
      EncryptionConfiguration userEnc = userConfig.getEncryption();
      defaultConfig.setEncryption(userEnc);
    }

    // Merge worker config
    if (userConfig.getWorker() != null) {
      WorkerConfiguration defaultWorker = defaultConfig.getWorker();
      WorkerConfiguration userWorker = userConfig.getWorker();
      //
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
    WorkerConfiguration.validate(config.getWorker());

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
