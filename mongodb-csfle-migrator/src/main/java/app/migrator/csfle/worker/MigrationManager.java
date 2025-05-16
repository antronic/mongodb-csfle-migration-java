package app.migrator.csfle.worker;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.slf4j.Logger;

import com.mongodb.client.MongoClient;

import app.migrator.csfle.config.Configuration;
import app.migrator.csfle.service.mongodb.MongoReader;
import app.migrator.csfle.service.mongodb.MongoWriter;

public class MigrationManager {
  private static final Logger logger =
      org.slf4j.LoggerFactory.getLogger(MigrationManager.class);
  private final WorkerManager workerManager;
  private final MongoReader sourceReader;
  private final MongoWriter targetWriter;

  private MongoClient sourceMongoClient;
  private MongoClient targetMongoClient;
  private String sourceDatabase;
  private String sourceCollection;

  private final Configuration configuration;

  private boolean isInitialized = false;
  private int batchSize = 1000;
  private int batchCount = 0;
  private long totalCount = 0;
  private int currentBatchSize = 0;
  private int currentBatchCount = 0;
  private int currentBatchIndex = 0;

  public MigrationManager(
      WorkerManager workerManager,
      Configuration configuration
      ) {
    this.workerManager = workerManager;
    this.configuration = configuration;
    this.sourceReader = new MongoReader();
    this.targetWriter = new MongoWriter();
  }

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

  public MigrationManager setup(
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

  public void run() {
    if (!isInitialized) {
      throw new IllegalStateException("MigrationManager is not initialized.");
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
    //
    currentBatchIndex = 0;
    currentBatchCount = getTotalRounds();
    //
    // Implement the logic to run the migration process
    // This could involve reading data from the source, processing it,
    // and writing it to the target database.
    sourceReader.setup(this.sourceMongoClient, sourceDatabase, sourceCollection);
    targetWriter.setup(this.targetMongoClient, sourceDatabase, sourceCollection);
    //
    // If total count is zero, just create the collection in the target database
    if (totalCount <= 0) {
      // Skip the validation if there are no documents to process
      logger.info("No documents to process in the source collection. {}", sourceCollection);
      if (configuration.getMigrationConfig().getMigrationOptions().isCreateCollectionEvenEmpty()) {
        logger.info("Creating Collection for {}.{}", sourceDatabase, sourceCollection);
        // Create the collection in the target database
        targetWriter.createCollection();
      } else {
        logger.info("Skipping migration for {}.{} as there are no documents to process.", sourceDatabase, sourceCollection);
        // If the collection is empty and we are not creating it, just return
        return;
      }
      return;
    }

    // Start the migration process
    for (int i = 0; i < batchCount; i++) {
      logger.info( "Batch: " + i  + " - " + sourceCollection);

      currentBatchIndex = i;
      currentBatchSize = Math.min(batchSize, (int) (totalCount - (i * batchSize)));
      currentBatchCount = Math.min(batchCount, this.getTotalRounds());

      // Read data from the source database and collection
      sourceReader.setSkip(currentBatchIndex * batchSize);
      sourceReader.setLimit(currentBatchSize);

      processBatch();
    }
  }

  private void processBatch() {
    // Read data from the source
    List<Document> docs = sourceReader.read().into(new ArrayList<>());

    logger.info("Target database: " + sourceDatabase + ", collection: " + sourceCollection);
    logger.info("Read " + docs.size() + " documents.");

    // Print all documents
    // logger.info("Documents:");
    // for (Document doc : docs) {
    //   // Process each document
    //   logger.info(doc.toJson());
    // }

    // Write data to the target
    targetWriter.writeBatch(docs);
  }

  public MigrationManager initialize() {
    // Initialize the migration process
    // This could involve setting up connections, preparing data structures, etc.
    this.batchSize = configuration.getWorker().getMaxBatchSize();
    this.totalCount = getTotalCountInCollection();
    this.batchCount = getTotalRounds();

    logger.info("Total docs count: " + totalCount);

    // this.currentBatchSize = Math.min(batchSize, (int) (totalCount - (currentBatchIndex * batchSize)));
    // this.currentBatchCount = Math.min(batchCount, this.getTotalRounds());
    // this.currentBatchIndex = 0;

    this.isInitialized = true;

    return this;
  }
}
