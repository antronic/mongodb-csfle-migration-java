package app.migrator.csfle;

import java.io.IOException;
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
import app.migrator.csfle.service.Report;
import app.migrator.csfle.worker.WorkerManager;
import app.migrator.csfle.worker.validation.ValidationManager;
import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * Main driver class for validating MongoDB collections after CSFLE migration.
 * Handles the setup, execution, and coordination of validation tasks across multiple collections.
 */
@Accessors(chain=true)
public class ValidationDriver {
  private final Logger logger = LoggerFactory.getLogger(ValidationDriver.class);
  private final Configuration config;
  private final WorkerManager workerManager;
  private final ValidationStrategy validationStrategy;
  //
  private Report report;
  //
  // MongoDB services for source and target databases
  private MongoDBService sourceService;
  private MongoDBService targetService;
  //
  // Map to hold collections to be validated
  private final Map<String, List<String>> collectionsMap = new HashMap<>();

  /**
   * Creates a new ValidationDriver with specified configuration and validation strategy.
   *
   * @param config The application configuration containing MongoDB connection details and worker settings
   * @param validationStrategy The validation strategy to use (COUNT, DOC_COMPARE)
   */
  public ValidationDriver(Configuration config, ValidationStrategy validationStrategy) {
    this.config = config;
    this.validationStrategy = validationStrategy;
    this.workerManager =
        new WorkerManager(config.getWorker().getMaxThreads(), config.getWorker().getMaxQueueSize());
  }

  /**
   * Starts the validation process for all configured collections.
   * Initializes workers, submits validation tasks, waits for completion,
   * generates reports, and performs cleanup.
   */
  public void start() {
    logger.info("\n\ncollectionsMap: {}\n", collectionsMap);
    // Initialize the worker manager with the maximum number of threads and queue size
    workerManager.initializeWorkers();
    //
    // Iterate over the collections map and submit validation tasks for each collection
    try {
      for (Map.Entry<String, List<String>> entry : collectionsMap.entrySet()) {
        //
        String dbName = entry.getKey();
        List<String> collections = entry.getValue();
        //
        // Check if the collections list is not empty
        for (String collectionName : collections) {
          this.startValidation(dbName, collectionName);
        }
      }

      logger.info("All tasks submitted. Waiting for completion...");
      workerManager.awaitTermination(); // Wait for all validation tasks to complete
    } finally {
      logger.info("All tasks completed");
      {
        try {
            this.report.generate();
        } catch (IOException ex) {
          logger.error("Error generating report: {}", ex.getMessage());
        }
      }
      shutdown();
    }
  }

  /**
   * Determines which validation strategy to use for a specific database collection.
   *
   * @param dbName The name of the database to validate
   * @param collectionName The name of the collection to validate
   */
  private void startValidation(String dbName, String collectionName) {
    logger.info("\n\ncollectionsMap: {}\n", collectionsMap);
    //==============================================================================
    // VALIDATION STRATEGIES
    //==============================================================================
    //
    // Choose the appropriate validation strategy based on configuration
    switch (this.validationStrategy) {
      case COUNT:
        this.startCount(dbName, collectionName);
        break;

      case DOC_COMPARE:
        this.startDocCompare(dbName, collectionName);
        break;
    }
  }

  /**
   * Initiates a count validation task for a specific database collection.
   * Creates a validation task that compares document counts between source and target collections.
   *
   * @param dbName The name of the database containing the collection
   * @param collectionName The name of the collection to validate
   */
  private void startCount(String dbName, String collectionName) {
    logger.info("Submitting counting task for {}.{}", dbName, collectionName);
    //
    // Submit the validation task to the worker manager
    workerManager.submitTask(collectionName, () -> {
      //
      // Initialize MongoDB clients for the validation task
      MongoClient sourceMongoClient = sourceService.getClient();
      MongoClient targetMongoClient = targetService.getClient();
      sourceMongoClient.getDatabase(dbName);
      targetMongoClient.getDatabase(dbName);
      //
      // Create a new instance of the ValidationManager for this task
      ValidationManager validationManager = new ValidationManager(ValidationStrategy.COUNT, workerManager, this.config);
      // Run the validation process
      validationManager.setup(sourceMongoClient, targetMongoClient, dbName, collectionName)
        .initialize()
        .setReport(report)
        .run();

      logger.info("Counting task completed for {}.{}", dbName, collectionName);
    });
  }

  private void startDocCompare(String dbName, String collectionName) {
    logger.info("Submitting document comparison task for {}.{}", dbName, collectionName);
    //
    // Submit the validation task to the worker manager
    workerManager.submitTask(collectionName, () -> {
      //
      // Initialize MongoDB clients for the validation task
      MongoClient sourceMongoClient = sourceService.getClient();
      MongoClient targetMongoClient = targetService.getClient();
      sourceMongoClient.getDatabase(dbName);
      targetMongoClient.getDatabase(dbName);
      //
      // Create a new instance of the ValidationManager for this task
      ValidationManager validationManager = new ValidationManager(ValidationStrategy.DOC_COMPARE, workerManager, this.config);
      // Run the validation process
      validationManager.setup(sourceMongoClient, targetMongoClient, dbName, collectionName)
        .initialize()
        .setReport(report)
        .run();

      logger.info("Document comparison task completed for {}.{}", dbName, collectionName);
    });

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
    // Load validation configuration
    ValidationConfiguration dbs = this.config.getValidationConfig();
    //
    // Initialize the report
    setupReport();
    //
    // Check if the validation configuration is valid
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

    return this;
  }

  /**
   * Configures the validation report format based on the selected validation strategy.
   * Sets up appropriate column headers for the report.
   */
  private void setupReport() {
    // Initialize the report
    this.report = new Report(this.validationStrategy.value);
    switch (this.validationStrategy) {
      case COUNT:
        this.report
          .setHeaders(new String[] { "Database", "Collection", "Source Count", "Target Count", "Result" });
        break;

      case DOC_COMPARE:
        this.report
          .setHeaders(new String[] { "Database", "Collection", "Total Source Documents", "Comparison Result" });
        // startDocCompare(dbName, collectionName);
        break;
    }
  }

  /**
   * Releases all resources used during validation.
   * Shuts down worker threads and closes MongoDB connections.
   */
  private void shutdown() {
    workerManager.shutdown();
    sourceService.close();
    targetService.close();
  }

  /**
   * Represents the available validation strategies for comparing MongoDB collections.
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
