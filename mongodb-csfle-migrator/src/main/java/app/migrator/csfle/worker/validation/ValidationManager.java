package app.migrator.csfle.worker.validation;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;

import app.migrator.csfle.ValidationDriver;
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
    if (batchSize <= 0) {
      throw new IllegalArgumentException("Batch size must be greater than zero.");
    }
    if (totalCount <= 0) {
      // Skip the validation if there are no documents to process
      logger.info("No documents to process in the source collection. {}", sourceCollection);
      logger.info("Skipping validation for {}.{}", sourceDatabase, sourceCollection);
      return;
    }
    if (this.report == null) {
      throw new IllegalStateException("Report is not set.");
    }
    //
    // Setup reader connections
    sourceReader.setup(this.sourceMongoClient, sourceDatabase, sourceCollection);
    targetReader.setup(this.targetMongoClient, sourceDatabase, sourceCollection);
    //
    //   Process the batch
    processBatch();
  }

  //----------------------------------------------------------------------
  // Batch Processing
  //----------------------------------------------------------------------

  /**
   * Processes batches of documents according to the selected validation strategy.
   * Handles validation logic and records results.
   */
  private void processBatch() {
    switch (this.validationStrategy) {
      case COUNT:
        logger.info("All tasks submitted. Waiting for completion...");
        // Perform counting validation
        ValidateByCount validateByCount = initializeCounting();
        validateByCount.count();
        //
        //----------------------------------------------------------------------
        // Results Analysis
        //----------------------------------------------------------------------
        long accumulatedSourceCount = validateByCount.getAccumulatedSourceCount();
        long accumulatedTargetCount = validateByCount.getAccumulatedTargetCount();

        String[] resultArr = new String[5];
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
        //
        // Add data to the report
        this.report.addData(resultArr);
        break;
      case DOC_COMPARE:
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
    this.totalCount = getTotalCountInCollection();
    this.batchCount = getTotalRounds();

    logger.info("{}.{} | Total docs count: {}", sourceDatabase, sourceCollection, totalCount);
    logger.info("{}.{} | Total batch count: {}", sourceDatabase, sourceCollection, batchCount);

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
   * Performs deeper validation by comparing document contents between source and target.
   * This approach verifies that documents were transferred completely and accurately.
   */
  public static class ValidateByDocCompare {
    /**
     * Creates a new document comparison validator.
     *
     * @param sourceDoc Source document to compare
     * @param targetDoc Target document to compare against
     */
    public ValidateByDocCompare(Document sourceDoc, Document targetDoc) {
    }

    /**
     * Compares two sets of documents by their _id field and contents.
     * Reports any missing or mismatched documents between source and target.
     *
     * @param sourceDocs List of documents from the source collection
     * @param targetDocs List of documents from the target collection
     */
    private void compareDocs(List<Document> sourceDocs, List<Document> targetDocs) {
      // Create a map of target documents by their _id for efficient lookups
      Map<Object, Document> targetById = targetDocs.stream()
          .collect(Collectors.toMap(doc -> doc.get("_id"), Function.identity()));

      // Compare each source document to its corresponding target document
      for (Document src : sourceDocs) {
          Object id = src.get("_id");
          Document tgt = targetById.get(id);

          if (tgt == null) {
              // Document exists in source but not in target
              logger.warn("Missing document in target: _id={}", id);
          } else if (!normalize(src).equals(normalize(tgt))) {
              // Documents exist in both, but contents don't match
              logger.warn("Mismatch at _id={}\nSRC: {}\nTGT: {}", id, src.toJson(), tgt.toJson());
          }
      }
    }

    /**
     * Normalizes document representation for consistent comparison.
     * Helps ensure that documents with the same logical content but different
     * representations are properly identified as matching.
     *
     * @param doc Document to normalize
     * @return Normalized string representation of the document
     */
    private String normalize(Document doc) {
        // Normalize by converting to canonical JSON (sorted keys if needed)
        // Optionally: strip metadata or transform to a stable hash
        return doc.toJson(); // or apply your own canonicalizer
    }
  }
}
