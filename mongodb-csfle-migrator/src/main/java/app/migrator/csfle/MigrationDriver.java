package app.migrator.csfle;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;

import app.migrator.csfle.config.Configuration;
import app.migrator.csfle.config.MigrationConfiguration;
import app.migrator.csfle.service.MongoCSFLE;
import app.migrator.csfle.service.MongoDBService;
import app.migrator.csfle.worker.MigrationManager;
import app.migrator.csfle.worker.WorkerManager;
import lombok.experimental.Accessors;

@Accessors(chain=true)
public class MigrationDriver {
  private final Logger logger = LoggerFactory.getLogger(MigrationDriver.class);
  private final Configuration config;
  private final WorkerManager workerManager;
  private MongoDBService sourceService;
  private MongoDBService targetService;
  //
  // Map to hold collections to be validated
  private final Map<String, List<String>> collectionsMap = new HashMap<>();
  //
  // Total number of tasks to be executed
  private int totalTasks;
  //
  // Latch to wait for all tasks to complete
  private CountDownLatch latch;

  public MigrationDriver(Configuration config) {
    this.config = config;
    this.workerManager =
        new WorkerManager(config.getWorker().getMaxThreads(), config.getWorker().getMaxQueueSize());
  }

  /**
   * Starts the migration process by initializing the worker manager and submitting tasks for each collection.
   */
  // This method is responsible for iterating over the collections to be migrated
  // and submitting migration tasks to the worker manager.
  public void startMigration() {
    // Initialize the worker manager with the maximum number of threads and queue size
    workerManager.initializeWorkers();
    //
    // Log the planned task count
    logger.info("Planned task count: {}", this.totalTasks);
    //
    // Iterate over the collections map and submit migration tasks for each collection
    try {
      for (Map.Entry<String, List<String>> entry : collectionsMap.entrySet()) {
        String dbName = entry.getKey();
        List<String> collections = entry.getValue();
        //
        // Check if the collections list is not empty
        for (String collectionName : collections) {
          logger.info("Submitting migration task for {}.{}", dbName, collectionName);
          //
          // Submit the migration task to the worker manager
          workerManager.submitTask(collectionName, () -> {
            // Create a new instance of the MigrationManager for each task
            MigrationManager migrationManager = new MigrationManager(workerManager, this.config);
            //
            // Initialize the migration manager with the source and target MongoDB clients
            // and the database and collection names
            MongoClient sourceMongoClient = sourceService.getClient();
            MongoClient targetMongoClient = targetService.getClient();
            sourceMongoClient.getDatabase(dbName);
            targetMongoClient.getDatabase(dbName);

            //
            // Run the migration process
            migrationManager
              .setup(sourceMongoClient, targetMongoClient, dbName, collectionName)
              .initialize()
              .run();
          }, latch);
        }
      }
      // Wait for all tasks to complete
      logger.info("Waiting for all tasks to complete...");
      latch.await();
      logger.info("All migration tasks completed.");
    } catch (InterruptedException e) {
      logger.error("Error while submitting migration tasks: {}", e.getMessage());
      e.printStackTrace();
    } finally {
      shutdown();
    }
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
    MigrationConfiguration dbs = this.config.getMigrationConfig();
    //
    // Check if the migration configuration is valid
    if (dbs != null) {
      for (Map.Entry<String, List<String>> entry : dbs.getTargetToMigrate().entrySet()) {
        String dbName = entry.getKey();
        List<String> collections = entry.getValue();

        if (collections != null && !collections.isEmpty()) {
          this.collectionsMap.put(dbName, collections);
        }
      }
    } else {
      logger.error("Migration configuration is null. Please check your configuration.");
      throw new RuntimeException("Migration configuration is null.");
    }
    //
    //
    if (this.collectionsMap.isEmpty()) {
      logger.error("No collections to migrate. Please check your configuration.");
      throw new RuntimeException("No collections to migrate.");
    }
    logger.info("Collections to migrate: {}", this.collectionsMap);
    //
    // Define tasks count and latch
    this.totalTasks = this.collectionsMap.values().stream().mapToInt(List::size).sum();
    this.latch = new CountDownLatch(this.totalTasks);
  }

  private void shutdown() {
    workerManager.shutdown();
    sourceService.close();
    targetService.close();
  }
}
