package app.migrator.csfle;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;

import app.migrator.csfle.config.Configuration;
import app.migrator.csfle.config.ValidationConfiguration;
import app.migrator.csfle.service.MongoCSFLE;
import app.migrator.csfle.service.MongoDBService;
import app.migrator.csfle.service.Report;
import app.migrator.csfle.worker.WorkerManager;
import app.migrator.csfle.worker.validation.ValidationManager;
import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * Main driver class for validating MongoDB collections after CSFLE migration.
 * Handles the setup, execution, and coordination of validation tasks across multiple collections.
 * Supports multiple validation strategies for comparing source and target data.
 */
@Accessors(chain=true)
public class ValidationDriver {
  private final Logger logger = LoggerFactory.getLogger(ValidationDriver.class);
  private final Configuration config;
  private final WorkerManager workerManager;
  private final ValidationStrategy validationStrategy;
  //
  // Report generator for validation results
  private Report report;
  //
  // MongoDB services for source and target databases
  private MongoDBService sourceService;
  private MongoDBService targetService;
  //
  // Map to hold collections to be validated (database name -> collection names)
  private final Map<String, List<String>> collectionsMap = new HashMap<>();
  //
  // Total number of validation tasks to be executed
  private int totalTasks;
  //
  // Latch to synchronize completion of all validation tasks
  private CountDownLatch latch;

  /**
   * Creates a new ValidationDriver with specified configuration and validation strategy.
   * Initializes the worker manager with configuration-based thread and queue limits.
   *
   * @param config The application configuration containing MongoDB connection details and worker settings
   * @param validationStrategy The validation strategy to use (COUNT, DOC_COMPARE)
   */
  public ValidationDriver(Configuration config, ValidationStrategy validationStrategy) {
    this.config = config;
    this.validationStrategy = validationStrategy;
    // Initialize worker manager with thread pool and queue size from configuration
    this.workerManager =
        new WorkerManager(config.getWorker().getMaxThreads(), config.getWorker().getMaxQueueSize());
  }

  /**
   * Starts the validation process for all configured collections.
   * Initializes workers, submits validation tasks, waits for completion,
   * generates reports, and performs cleanup.
   */
  public void start() {
    logger.info("\n\nCollections Map: {}\n", this.collectionsMap);
    // Initialize the worker thread pool
    workerManager.initializeWorkers();
    //
    // Log the planned task count
    logger.info("Planned validation task count: {}", this.totalTasks);
    //
    // Iterate over the collections map and submit validation tasks for each collection
    try {
      for (Map.Entry<String, List<String>> entry : this.collectionsMap.entrySet()) {
        //
        String dbName = entry.getKey();
        List<String> collections = entry.getValue();
        //
        // Submit validation tasks for each collection in the database
        for (String collectionName : collections) {
          this.startValidation(dbName, collectionName);
        }
      }

      // Wait for all tasks to complete using the countdown latch
      logger.info("Waiting for all validation tasks to complete...");
      this.latch.await();
      logger.info("All validation tasks completed successfully.");

    } catch (InterruptedException e) {
      logger.error("Error while waiting for validation tasks to complete: {}", e.getMessage());
      Thread.currentThread().interrupt(); // Restore interrupted state
      e.printStackTrace();
    } finally {
      logger.debug("All tasks completed, generating report and cleaning up resources");
      // Generate the validation report with results
      try {
          this.report.generate();
      } catch (IOException ex) {
        logger.error("Error generating validation report: {}", ex.getMessage());
      }
      // Release all resources
      shutdown();
    }
  }

  /**
   * Submits a validation task for a specific database collection to the worker pool.
   * Creates a new validation manager instance for each collection to isolate validation state.
   *
   * @param dbName The name of the database to validate
   * @param collectionName The name of the collection to validate
   * @throws InterruptedException if the thread is interrupted while submitting task
   */
  private void startValidation(String dbName, String collectionName) throws InterruptedException {
    //==============================================================================
    // VALIDATION STRATEGIES
    //==============================================================================
    // Log validation task submission
    logger.info("Submitting {} validation task for {}.{}",
        this.validationStrategy.toString(), dbName, collectionName);
    //
    // Submit the validation task to the worker manager with the collection name as task ID
    workerManager.submitTask(collectionName, () -> {
      //
      // Get fresh MongoDB client connections for this validation task
      MongoClient sourceMongoClient = sourceService.getClient();
      MongoClient targetMongoClient = targetService.getClient();
      // Initialize database connections
      sourceMongoClient.getDatabase(dbName);
      targetMongoClient.getDatabase(dbName);
      //
      // Create a new isolated ValidationManager instance for this task
      ValidationManager validationManager = new ValidationManager(
          this.validationStrategy, workerManager, this.config);
      // Configure and run the validation process
      validationManager.setup(sourceMongoClient, targetMongoClient, dbName, collectionName)
        .initialize()  // Initialize with collection statistics and batch setup
        .setReport(report)  // Provide report for recording results
        .run();  // Execute validation process

      logger.info("Validation task completed for {}.{}", dbName, collectionName);
    }, this.latch);  // CountDownLatch decrements when task completes
  }

  /**
   * Sets up the validation driver by initializing MongoDB connections and preparing collections.
   * Configures CSFLE for target MongoDB, initializes source and target connections,
   * loads validation configuration, and prepares the report.
   *
   * @return This ValidationDriver instance (for method chaining)
   * @throws RuntimeException if configuration is invalid or no collections are configured for validation
   */
  public ValidationDriver setup() {
    //
    // Initialize CSFLE (Client-Side Field Level Encryption) client for target MongoDB
    MongoCSFLE csfleClient = new MongoCSFLE(config.getTargetMongoDB().getUri(), config);
    csfleClient.setup();
    //
    // Get preconfigured target MongoDB client settings builder with CSFLE enabled
    MongoClientSettings.Builder targetMongoClientBuilder = csfleClient.getMongoClientSettingsBuilder();
    //
    // Initialize source MongoDB client (standard, without CSFLE)
    sourceService = new MongoDBService(config.getSourceMongoDB());
    // Initialize target MongoDB client with CSFLE capabilities
    targetService = new MongoDBService(config.getTargetMongoDB(), targetMongoClientBuilder);
    //
    // Setup source and target MongoDB service connections
    sourceService.setup();
    targetService.setup();
    //
    // Load validation configuration that specifies which collections to validate
    ValidationConfiguration dbs = this.config.getValidationConfig();
    //
    // Initialize the validation report with appropriate format
    setupReport();
    //
    // Process validation configuration and build collections map
    if (dbs != null) {
      for (Map.Entry<String, List<String>> entry : dbs.getTargetToValidate().entrySet()) {
        String dbName = entry.getKey();
        List<String> collections = entry.getValue();
        //
        // Add non-empty collection lists to the validation map
        if (collections != null && !collections.isEmpty()) {
          //
          logger.debug("Adding {}.{} to collections map", dbName, collections);
          this.collectionsMap.put(dbName, collections);
        }
      }
      //
      // Calculate total validation tasks and initialize completion latch
      this.totalTasks = this.collectionsMap.values().stream().mapToInt(List::size).sum();
      this.latch = new CountDownLatch(this.totalTasks);
    } else {
      logger.error("Validation configuration is null. Please check your configuration.");
      throw new RuntimeException("Validation configuration is null.");
    }
    //
    // Verify we have collections to validate
    if (this.collectionsMap.isEmpty()) {
      logger.error("No collections to validate. Please check your configuration.");
      throw new RuntimeException("No collections to validate.");
    }
    logger.info("Collections to validate: {}", this.collectionsMap);

    return this;
  }

  /**
   * Configures the validation report format based on the selected validation strategy.
   * Sets up appropriate column headers for the report to match validation results.
   */
  private void setupReport() {
    // Initialize report with validation strategy name
    this.report = new Report(this.validationStrategy.value);

    // Configure report columns based on validation strategy
    switch (this.validationStrategy) {
      case COUNT:
        // For count strategy: track database, collection, source count, target count, match result
        this.report
          .setHeaders(new String[] { "Database", "Collection", "Source Count", "Target Count", "Result" });
        break;

      case DOC_COMPARE:
        // For doc_compare strategy: track database, collection, total docs, detailed comparison result
        this.report
          .setHeaders(new String[] { "Database", "Collection", "Total Source Documents", "Comparison Result" });
        break;
    }
  }

  /**
   * Releases all resources used during validation.
   * Shuts down worker threads and closes MongoDB connections to prevent resource leaks.
   */
  private void shutdown() {
    logger.debug("Shutting down validation resources");
    workerManager.shutdown();  // Shutdown worker thread pool
    sourceService.close();     // Close source MongoDB connections
    targetService.close();     // Close target MongoDB connections
  }

  /**
   * Represents the available validation strategies for comparing MongoDB collections.
   * Each strategy provides different levels of validation detail and performance characteristics.
   */
  public static enum ValidationStrategy {
    COUNT("count"),           // Compare document counts between source and target collections
    DOC_COMPARE("doc_compare"); // Compare individual document contents between collections

    @Getter
    public final String value;

    ValidationStrategy(String value) {
      this.value = value;
    }
  }
}
