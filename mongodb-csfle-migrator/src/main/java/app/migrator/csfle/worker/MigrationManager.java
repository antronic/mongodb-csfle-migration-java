package app.migrator.csfle.worker;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.slf4j.Logger;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;

import app.migrator.csfle.common.Constants;
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
    // If readOperationType is "skip", we need to ensure the total count is set
    // If total count is zero, just create the collection in the target database
    if (this.configuration.getWorker().getReadOperationType().equals(Constants.ReadOperationType.SKIP) && totalCount <= 0) {
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
    //
    //
    logger.info("[START_MIG_COLL] {}.{} - Start process (date: {})", sourceDatabase, sourceCollection, new Date());
    //
    // Start the migration process
    if (this.configuration.getWorker().getReadOperationType().equals(Constants.ReadOperationType.SKIP)) {
      logger.info("Processing batch (skip,limit) for {}.{}", sourceDatabase, sourceCollection);
      for (int i = 0; i < batchCount; i++) {
        logger.info( "Batch: " + i  + " - " + sourceCollection);

        currentBatchIndex = i;
        currentBatchSize = Math.min(batchSize, (int) (totalCount - (i * batchSize)));
        currentBatchCount = Math.min(batchCount, this.getTotalRounds());

        // Read data from the source database and collection
        sourceReader.setSkip(currentBatchIndex * batchSize);
        sourceReader.setLimit(currentBatchSize);

        Timestamp lastMs = new Timestamp(new Date().getTime());
        // Use skip and limit to read data
        processBatch();
        //
        Timestamp current = new Timestamp(new Date().getTime());
        logger.info("Written (Skip) batch to target: {}.{} - Batch size: {} - Time taken: {} ms",
            sourceDatabase, sourceCollection, currentBatchSize, current.getTime() - lastMs.getTime());
      }
    } else if (this.configuration.getWorker().getReadOperationType().equals(Constants.ReadOperationType.CURSOR)) {
      // Use cursor to read data
      logger.info("Processing cursor for {}.{}", sourceDatabase, sourceCollection);
      processBatchByCursor();
    }

    logger.info("[END_MIG_COLL] {}.{} - End process (date: {})", sourceDatabase, sourceCollection, new Date());
  }

  private void processBatch() {
    // Read data from the source
    List<Document> docs = sourceReader.read().into(new ArrayList<>());
    //
    logger.info("Reading: [" + sourceDatabase + "." + sourceCollection + "]" + docs.size() + " documents.");
    //
    // Print all documents
    // logger.info("Documents:");
    // for (Document doc : docs) {
    //   // Process each document
    //   logger.info(doc.toJson());
    // }
    //
    // Retry until it completes successfully
    // Write data to the target
    targetWriter.writeBatch(docs);
  }

  private void processBatchByCursor() {
    MongoCursor<Document> cursor = sourceReader.readWithCursor();
    List<Document> batchDocs = new ArrayList<>(this.batchSize);
    //
    // Read documents from the cursor
    while (cursor.hasNext()) {
      Timestamp lastMs = new Timestamp(new Date().getTime());
      Document doc = cursor.next();
      batchDocs.add(doc);
      //
      // Process the document
      if (batchDocs.size() >= this.batchSize || !cursor.hasNext()) {
        // logger.info(sourceDatabase);
        // Write the batch to the target
        targetWriter.writeBatch(batchDocs);
        //
        Timestamp current = new Timestamp(new Date().getTime());
        logger.info("Written (Cursor) batch to target: {}.{} - Batch size: {} - Time taken: {} ms",
            sourceDatabase, sourceCollection, batchDocs.size(), current.getTime() - lastMs.getTime());
        // Empty the batch
        batchDocs.clear();
      }
    }

  }

  public MigrationManager initialize() {
    // Initialize the migration process
    // This could involve setting up connections, preparing data structures, etc.
    this.batchSize = configuration.getWorker().getMaxBatchSize();
    //
    // Read operation type == skip
    // requires totalCount and batchCount
    if (this.configuration.getWorker().getReadOperationType().equals(Constants.ReadOperationType.SKIP)) {
      this.totalCount = getTotalCountInCollection();
      this.batchCount = getTotalRounds();
      logger.info("Total docs count: " + totalCount);
    }


    // this.currentBatchSize = Math.min(batchSize, (int) (totalCount - (currentBatchIndex * batchSize)));
    // this.currentBatchCount = Math.min(batchCount, this.getTotalRounds());
    // this.currentBatchIndex = 0;

    this.isInitialized = true;

    return this;
  }
}
