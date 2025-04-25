package app.migrator.csfle.worker.validation;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;

import app.migrator.csfle.config.Configuration;
import app.migrator.csfle.service.mongodb.MongoReader;
import app.migrator.csfle.worker.WorkerManager;

/**
 * ValidationManager handles the comparison of data between source and target MongoDB instances.
 * It provides mechanisms to validate data consistency across databases through various validation strategies.
 */
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


  //----------------------------------------------------------------------
  // Processing State
  //----------------------------------------------------------------------

  private boolean isInitialized = false;
  private int batchSize = 1000;
  private int batchCount = 0;
  private long totalCount = 0;

  private boolean isMatched = false;

  private final ValidationStrategy validationStrategy;

  //----------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------

  public ValidationManager(
      ValidationStrategy validationStrategy,
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

  private long getTotalCountInCollection() {
    // Implement the logic to count the total number of documents in the source collection
    // This could involve using the MongoDB Java driver to query the collection.
    return this.sourceMongoClient
        .getDatabase(sourceDatabase)
        .getCollection(sourceCollection)
        .countDocuments();
  }

  private int getTotalRounds() {
    return (int) Math.ceil((double) totalCount / (double) batchSize);
  }

  //----------------------------------------------------------------------
  // Setup and Configuration
  //----------------------------------------------------------------------

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

    // currentBatchIndex = 0;

    // Setup reader connections
    sourceReader.setup(this.sourceMongoClient, sourceDatabase, sourceCollection);
    targetReader.setup(this.targetMongoClient, sourceDatabase, sourceCollection);

    // Process data in batches
    // for (int i = 0; i < batchCount; i++) {
    //   logger.info("Batch: " + i  + " - " + sourceCollection);

    //   // Set the skip and limit for the source and target readers
    //   currentBatchIndex = i;
    //   currentBatchSize = Math.min(batchSize, (int) (totalCount - (i * batchSize)));

    //   // Configure readers for current batch
    //   sourceReader.setSkip(currentBatchIndex * batchSize);
    //   sourceReader.setLimit(currentBatchSize);

    //   targetReader.setSkip(currentBatchIndex * batchSize);
    //   targetReader.setLimit(currentBatchSize);

    //   logger.info("-----------------------");
    //   logger.info("Batch: {} | Source ns: {}.{}", currentBatchIndex, sourceDatabase, sourceCollection);
    //   logger.info("Limit: " + sourceReader.getLimit());
    //   logger.info("Skip: " + sourceReader.getSkip());
    //   logger.info("-----------------------");

    //   Process the batch
    processBatch();
    // }
  }

  //----------------------------------------------------------------------
  // Batch Processing
  //----------------------------------------------------------------------
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

        logger.info("{}.{} Result count: Source = {}, Target = {}", sourceDatabase, sourceCollection, accumulatedSourceCount, accumulatedTargetCount);

        if (accumulatedSourceCount != accumulatedTargetCount) {
          logger.warn("{}.{} Document count mismatch: source={}, target={}", sourceDatabase, sourceCollection, accumulatedSourceCount, accumulatedTargetCount);
        } else {
          logger.info("{}.{} Document count match: {}", sourceDatabase, sourceCollection, accumulatedSourceCount);
        }
        logger.info("{}.{} Counting task completed for {}.{}", sourceDatabase, sourceCollection, sourceDatabase, sourceCollection);
        break;
  // Perform document comparison validation
  // ValidateByDocCompare validateByDocCompare = new ValidateByDocCompare(null, null);
  // validateByDocCompare.compareDocs(sourceReader.read(), targetReader.read());
      case DOC_COMPARE:
          break;
      default:
          throw new IllegalArgumentException("Unknown validation strategy: " + this.validationStrategy);
    }
  }

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
    public ValidateByDocCompare(Document sourceDoc, Document targetDoc) {
    }

    private void compareDocs(List<Document> sourceDocs, List<Document> targetDocs) {
      Map<Object, Document> targetById = targetDocs.stream()
          .collect(Collectors.toMap(doc -> doc.get("_id"), Function.identity()));

      for (Document src : sourceDocs) {
          Object id = src.get("_id");
          Document tgt = targetById.get(id);

          if (tgt == null) {
              logger.warn("Missing document in target: _id={}", id);
          } else if (!normalize(src).equals(normalize(tgt))) {
              logger.warn("Mismatch at _id={}\nSRC: {}\nTGT: {}", id, src.toJson(), tgt.toJson());
          }
      }
    }

    private String normalize(Document doc) {
        // Normalize by converting to canonical JSON (sorted keys if needed)
        // Optionally: strip metadata or transform to a stable hash
        return doc.toJson(); // or apply your own canonicalizer
    }
  }

  public static enum ValidationStrategy {
    COUNT,
    DOC_COMPARE
  }
}
