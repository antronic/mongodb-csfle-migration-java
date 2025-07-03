package app.migrator.csfle.worker.validation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;

import app.migrator.csfle.ValidationDriver;
import app.migrator.csfle.common.Constants;
import app.migrator.csfle.config.Configuration;
import app.migrator.csfle.service.Report;
import app.migrator.csfle.service.mongodb.MongoReader;
import app.migrator.csfle.worker.WorkerManager;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * ValidationManager handles the comparison of data between source and target MongoDB instances.
 * It provides mechanisms to validate data consistency across databases through various validation strategies.
 * This class coordinates the validation process, manages batch processing, and generates validation reports.
 */
@Accessors(chain = true)
public class ValidationManager {
  //----------------------------------------------------------------------
  // Class Variables
  //----------------------------------------------------------------------
  private static final Logger logger =
      LoggerFactory.getLogger(ValidationManager.class);
  private final WorkerManager workerManager;
  private final MongoReader sourceReader;
  private final MongoReader targetReader;

  private MongoClient sourceMongoClient;
  private MongoClient targetMongoClient;
  private String sourceDatabase;
  private String sourceCollection;

  private final Configuration configuration;
  @Getter
  @Setter
  private Report report;

  // private List<Map<String, List<String>>> validationResults;


  //----------------------------------------------------------------------
  // Processing State
  //----------------------------------------------------------------------

  private boolean isInitialized = false;
  private int batchSize = 1000;      // Default batch size for document processing
  private int batchCount = 0;        // Number of batches to process
  private long totalCount = 0;       // Total number of documents in the collection

  private boolean isMatched = false; // Flag indicating if source and target data match

  private final ValidationDriver.ValidationStrategy validationStrategy;

  //----------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------

  /**
   * Creates a new ValidationManager with the specified validation strategy and configuration.
   *
   * @param validationStrategy The strategy to use for validating data (COUNT, DOC_COMPARE)
   * @param workerManager The worker manager for handling concurrent validation tasks
   * @param configuration The application configuration with validation settings
   */
  public ValidationManager(
      ValidationDriver.ValidationStrategy validationStrategy,
      WorkerManager workerManager,
      Configuration configuration
      ) {
    // Initialize the validation strategy
    this.validationStrategy = validationStrategy;
    // Initialize the worker manager and configuration
    this.workerManager = workerManager;
    this.configuration = configuration;
    // Initialize the source and target readers
    this.sourceReader = new MongoReader();
    this.targetReader = new MongoReader();
  }

  //----------------------------------------------------------------------
  // Utility Methods
  //----------------------------------------------------------------------

  /**
   * Gets the total count of documents in the source collection.
   *
   * @return The total number of documents
   */
  private long getTotalCountInCollection() {
    // Get the total count of documents in the source collection using MongoDB's countDocuments
    return this.sourceMongoClient
        .getDatabase(sourceDatabase)
        .getCollection(sourceCollection)
        .countDocuments();
  }

  /**
   * Calculates the total number of batches needed based on document count and batch size.
   *
   * @return Total number of batches needed for processing
   */
  private int getTotalRounds() {
    return (int) Math.ceil((double) totalCount / (double) batchSize);
  }

  //----------------------------------------------------------------------
  // Setup and Configuration
  //----------------------------------------------------------------------

  /**
   * Sets up the validation manager with MongoDB clients and collection information.
   *
   * @param sourceMongoClient The MongoDB client for the source database
   * @param targetMongoClient The MongoDB client for the target database
   * @param sourceDatabase The name of the source database
   * @param sourceCollection The name of the source collection
   * @return This ValidationManager instance for method chaining
   */
  public ValidationManager setup(
      MongoClient sourceMongoClient,
      MongoClient targetMongoClient,
      String sourceDatabase,
      String sourceCollection) {
    this.sourceMongoClient = sourceMongoClient;
    this.targetMongoClient = targetMongoClient;
    this.sourceDatabase = sourceDatabase;
    this.sourceCollection = sourceCollection;

    return this;
  }

  //----------------------------------------------------------------------
  // Validation Execution
  //----------------------------------------------------------------------

  /**
   * Executes the validation process based on the selected validation strategy.
   * Validates input parameters, sets up connections, processes data in batches,
   * and records results in the validation report.
   *
   * @throws IllegalStateException if the validation manager is not properly initialized
   * @throws IllegalArgumentException if validation parameters are invalid
   */
  public void run() {
    if (!isInitialized) {
      throw new IllegalStateException("ValidationManager is not initialized.");
    }
    if (sourceMongoClient == null || targetMongoClient == null) {
      throw new IllegalStateException("MongoDB clients are not set up.");
    }
    if (sourceDatabase == null || sourceCollection == null) {
      throw new IllegalStateException("Source database or collection is not set.");
    }
    if (this.report == null) {
      throw new IllegalStateException("Report is not set.");
    }

    // Validate read operation type
    if (configuration.getWorker().getReadOperationType().equals(Constants.ReadOperationType.SKIP)) {
      if (batchSize <= 0) {
        throw new IllegalArgumentException("Batch size must be greater than zero.");
      }
    }

    //
    // Setup reader connections
    sourceReader.setup(this.sourceMongoClient, sourceDatabase, sourceCollection);
    targetReader.setup(this.targetMongoClient, sourceDatabase, sourceCollection);
    //
    // Process the batch
    if (totalCount <= 0 && (this.validationStrategy == ValidationDriver.ValidationStrategy.DOC_COUNT || configuration.getWorker().getReadOperationType().equals(Constants.ReadOperationType.SKIP))) {
      // Skip the validation if there are no documents to process
      logger.info("No documents to process in the source collection. {}", sourceCollection);
      if (!configuration.getValidationConfig().getValidationOptions().isValidateEmptyCollections()) {
        logger.info("Skipping validation for {}.{}", sourceDatabase, sourceCollection);
        return;
      }
    }

    switch (configuration.getWorker().getReadOperationType()) {
      case Constants.ReadOperationType.CURSOR:
        // Handle cursor-based reading
        processBatch();
        break;
      case Constants.ReadOperationType.SKIP:
        // Handle skip-based reading
        processBatch();
        break;
      default:
        throw new IllegalArgumentException("Unknown read operation type.");
    }
  }

  //----------------------------------------------------------------------
  // Batch Processing
  //----------------------------------------------------------------------

  /**
   * Processes batches of documents according to the selected validation strategy.
   * Handles validation logic and records results.
   */
  private void processBatch() {
    logger.info("All tasks submitted. Waiting for completion...");
    switch (this.validationStrategy) {
      // =========== Counting ==============
      case DOC_COUNT:
        {
          // Perform counting validation
          ValidateByCount validateByCount = initializeCounting();
          long startTime = System.currentTimeMillis();
          validateByCount.count();
          long endTime = System.currentTimeMillis();
          //
          //----------------------------------------------------------------------
          // Results Analysis
          //----------------------------------------------------------------------
          long accumulatedSourceCount = validateByCount.getAccumulatedSourceCount();
          long accumulatedTargetCount = validateByCount.getAccumulatedTargetCount();

          String[] resultArr = new String[6];
          resultArr[0] = sourceDatabase; // database
          resultArr[1] = sourceCollection; // collection
          resultArr[2] = String.valueOf(accumulatedSourceCount); // source count
          resultArr[3] = String.valueOf(accumulatedTargetCount); // target count

          logger.info("{}.{} Result count: Source = {}, Target = {}", sourceDatabase, sourceCollection, accumulatedSourceCount, accumulatedTargetCount);

          if (accumulatedSourceCount != accumulatedTargetCount) {
            resultArr[4] = "Mismatch"; // result
            logger.warn("{}.{} Document count mismatch: source={}, target={}", sourceDatabase, sourceCollection, accumulatedSourceCount, accumulatedTargetCount);
          } else {
            resultArr[4] = "Match"; // result
            logger.info("{}.{} Document count match: {}", sourceDatabase, sourceCollection, accumulatedSourceCount);
          }
          logger.info("{}.{} Counting task completed for {}.{}", sourceDatabase, sourceCollection, sourceDatabase, sourceCollection);

          long took = endTime - startTime;
          resultArr[5] = String.valueOf(took); // took in ms
          //
          // Add data to the report
          this.report.addData(resultArr);
        }
        break;
      // =========== Document Comparison ==============
      case DOC_COMPARE:
        {
          ValidateByDocCompare validateByDocCompare = initializeDocCompare();
          validateByDocCompare.setReadOperateionType(
            configuration.getWorker().getReadOperationType()
            );

          long startTime = System.currentTimeMillis();
          validateByDocCompare.run();
          long endTime = System.currentTimeMillis();

          String[] resultArr = new String[5];
          resultArr[0] = sourceDatabase; // database
          resultArr[1] = sourceCollection; // collection
          resultArr[2] = String.valueOf(validateByDocCompare.getTotalDocs()); // total docs

          boolean isValid = validateByDocCompare.isValid();
          resultArr[3] = isValid ? "Match" : "Mismatch"; // result
          resultArr[4] = String.valueOf(endTime - startTime); // tooks in ms

          this.report.addData(resultArr);

          logger.info("Document comparison validation result: {}", isValid ? "Valid" : "Invalid");
        }
        break;
      default:
          throw new IllegalArgumentException("Unknown validation strategy: " + this.validationStrategy);
    }
  }

  /**
   * Initializes the validation manager by setting up batch processing parameters.
   * Calculates the total document count and required batch count based on the configured batch size.
   *
   * @return This ValidationManager instance for method chaining
   */
  public ValidationManager initialize() {
    // Initialize the validation process
    this.batchSize = configuration.getWorker().getMaxBatchSize();
    // If validation strategy is document count, or read operation type is skip
    if (this.validationStrategy == ValidationDriver.ValidationStrategy.DOC_COUNT || configuration.getWorker().getReadOperationType().equals(Constants.ReadOperationType.SKIP)) {
      this.totalCount = getTotalCountInCollection();
      this.batchCount = getTotalRounds();

      logger.info("{}.{} | Total docs count: {}", sourceDatabase, sourceCollection, totalCount);
      logger.info("{}.{} | Total batch count: {}", sourceDatabase, sourceCollection, batchCount);
    }

    this.isInitialized = true;

    return this;
  }

  /**
   * Creates and initializes a ValidateByCount instance for document count validation.
   *
   * @return Configured ValidateByCount instance ready to perform count validation
   */
  private ValidateByCount initializeCounting() {
    ValidateByCount validateByCount = new ValidateByCount(sourceReader, targetReader);
    validateByCount
      .setBatchSize(batchSize)
      .setTotalBatch(batchCount)
      .setTotalDocs(totalCount);

    return validateByCount;
  }

  /**
   * Creates and initializes a ValidateByDocCompare instance for document content validation.
   *
   * @return Configured ValidateByDocCompare instance ready to perform document comparison validation
   */
  private ValidateByDocCompare initializeDocCompare() {
    ValidateByDocCompare validateByDocCompare = new ValidateByDocCompare(sourceReader, targetReader);
    validateByDocCompare
      .setBatchSize(batchSize)
      .setTotalBatch(batchCount)
      .setTotalDocs(totalCount);

    return validateByDocCompare;
  }
}
