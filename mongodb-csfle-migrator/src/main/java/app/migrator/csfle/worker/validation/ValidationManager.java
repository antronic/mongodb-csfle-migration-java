package app.migrator.csfle.worker.validation;

import java.util.LinkedHashMap;
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
  private int batchSize = 1000;
  private int batchCount = 0;
  private long totalCount = 0;

  private boolean isMatched = false;

  private final ValidationDriver.ValidationStrategy validationStrategy;

  //----------------------------------------------------------------------
  // Constructor
  //----------------------------------------------------------------------

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

        Map<String, String> mapResult = new LinkedHashMap<>();
        mapResult.put("database", sourceDatabase);
        mapResult.put("collection", sourceCollection);
        mapResult.put("sourceCount", String.valueOf(accumulatedSourceCount));
        mapResult.put("targetCount", String.valueOf(accumulatedTargetCount));

        logger.info("{}.{} Result count: Source = {}, Target = {}", sourceDatabase, sourceCollection, accumulatedSourceCount, accumulatedTargetCount);

        if (accumulatedSourceCount != accumulatedTargetCount) {
          mapResult.put("result", "Mismatch");
          logger.warn("{}.{} Document count mismatch: source={}, target={}", sourceDatabase, sourceCollection, accumulatedSourceCount, accumulatedTargetCount);
        } else {
          mapResult.put("result", "Match");
          logger.info("{}.{} Document count match: {}", sourceDatabase, sourceCollection, accumulatedSourceCount);
        }
        logger.info("{}.{} Counting task completed for {}.{}", sourceDatabase, sourceCollection, sourceDatabase, sourceCollection);
        //
        // Add data to the report
        this.report.addData(mapResult.values().toArray(new String[0]));
        // Create a new report for the validation results
        logger.info("Result Map: {}", mapResult);
        break;
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
}
