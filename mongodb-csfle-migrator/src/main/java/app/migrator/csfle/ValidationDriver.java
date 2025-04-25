package app.migrator.csfle;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;

import app.migrator.csfle.config.Configuration;
import app.migrator.csfle.config.ValidationConfiguration;
import app.migrator.csfle.service.MongoCSFLE;
import app.migrator.csfle.service.MongoDBService;
import app.migrator.csfle.worker.WorkerManager;
import app.migrator.csfle.worker.validation.ValidationManager;

public class ValidationDriver {
  private final Logger logger = LoggerFactory.getLogger(ValidationDriver.class);
  private final Configuration config;
  private final WorkerManager workerManager;
  //
  // MongoDB services for source and target databases
  private MongoDBService sourceService;
  private MongoDBService targetService;
  //
  // Map to hold collections to be validated
  private final Map<String, List<String>> collectionsMap = new HashMap<>();
  //
  public ValidationDriver(Configuration config) {
    this.config = config;
    this.workerManager =
        new WorkerManager(config.getWorker().getMaxThreads(), config.getWorker().getMaxQueueSize());
  }

  /**
   * Starts the validation process by initializing the worker manager and submitting tasks for each collection.
   */
  // This method is responsible for iterating over the collections to be validated
  // and submitting validation tasks to the worker manager.
  public void startCount() {
    logger.info("\n\ncollectionsMap: {}\n", collectionsMap);
    // Initialize the worker manager with the maximum number of threads and queue size
    workerManager.initializeWorkers();
    //
    // Iterate over the collections map and submit migration tasks for each collection
    try {
      for (Map.Entry<String, List<String>> entry : collectionsMap.entrySet()) {
        String dbName = entry.getKey();
        List<String> collections = entry.getValue();
        //
        // Check if the collections list is not empty
        for (String collectionName : collections) {
          logger.info("Submitting counting task for {}.{}", dbName, collectionName);
          //
          // Submit the migration task to the worker manager
          workerManager.submitTask(collectionName, () -> {
            //
            // Initialize the migration manager with the source and target MongoDB clients
            // and the database and collection names
            MongoClient sourceMongoClient = sourceService.getClient();
            MongoClient targetMongoClient = targetService.getClient();
            sourceMongoClient.getDatabase(dbName);
            targetMongoClient.getDatabase(dbName);
            //
            // Create a new instance of the ValidationManager for each task
            ValidationManager validationManager = new ValidationManager(ValidationManager.ValidationStrategy.COUNT, workerManager, this.config);
            // Run the migration process
            validationManager.setup(sourceMongoClient, targetMongoClient, dbName, collectionName)
              .initialize()
              .run();

            logger.info("Counting task completed for {}.{}", dbName, collectionName);
          });
        }
      }

      logger.info("All tasks submitted. Waiting for completion...");
      // workerManager.awaitTermination();
    } finally {
      shutdown();
    }
  }

  public void testConcurrent() {
    workerManager.initializeWorkers();

    for (int i = 0; i < 10; i++) {
      workerManager.submitTask("i---" + i, () -> {
        try {
          Thread.sleep(1000);

          logger.info("Task completed {}", Thread.currentThread().getName());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
    }

    logger.info("All tasks submitted. Waiting for completion...");

    workerManager.shutdown();
    logger.info("All tasks completed");
  }

  /**
   * Sets up the migration driver by initializing the source and target MongoDB clients.
   * It also loads the migration configuration and prepares the collections to be migrated.
   */
  public void setup() {
     //
    // Initialize CSFLE client for target MongoDB
    MongoCSFLE csfleClient = new MongoCSFLE(config.getTargetMongoDB().getUri(), config);
    csfleClient.setup();
    //
    // Initialize target MongoDB client with CSFLE
    MongoClientSettings.Builder targetMongoClientBuilder = csfleClient.getMongoClientSettingsBuilder();
    //
    // Initialize source and target MongoDB clients
    //
    // Initialize source MongoDB client
    sourceService = new MongoDBService(config.getSourceMongoDB());
    // Initialize target MongoDB client with CSFLE
    targetService = new MongoDBService(config.getTargetMongoDB(), targetMongoClientBuilder);
    //
    // Setup source and target MongoDB services
    sourceService.setup();
    targetService.setup();
    //
    // Load migration configuration
    ValidationConfiguration dbs = this.config.getValidationConfig();
    //
    // Check if the migration configuration is valid
    if (dbs != null) {
      for (Map.Entry<String, List<String>> entry : dbs.getTargetToValidate().entrySet()) {
        String dbName = entry.getKey();
        List<String> collections = entry.getValue();
        //
        // Check if the collections list is not empty
        if (collections != null && !collections.isEmpty()) {
          //
          // Add the database name and collections to the collections map
          logger.info("Adding {}.{} to collections map", dbName, collections);
          this.collectionsMap.put(dbName, collections);
        }
      }
    } else {
      logger.error("Validation configuration is null. Please check your configuration.");
      throw new RuntimeException("Validation configuration is null.");
    }
    //
    //
    if (this.collectionsMap.isEmpty()) {
      logger.error("No collections to validate. Please check your configuration.");
      throw new RuntimeException("No collections to validate.");
    }
    logger.info("Collections to validate: {}", this.collectionsMap);
  }

  private void shutdown() {
    workerManager.shutdown();
    sourceService.close();
    targetService.close();
  }
}
