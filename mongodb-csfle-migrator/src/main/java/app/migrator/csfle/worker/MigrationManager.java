package app.migrator.csfle.worker;

import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.slf4j.Logger;
import com.mongodb.client.MongoClient;
import app.migrator.csfle.config.Configuration;

public class MigrationManager {
  private static final Logger logger =
      org.slf4j.LoggerFactory.getLogger(MigrationManager.class);
  private final WorkerManager workerManager;
  private final MigrationSourceReader sourceReader;
  private final MigrationTargetWriter targetWriter;

  private MongoClient sourceMongoClient;
  private MongoClient targetMongoClient;
  private String sourceDatabase;
  private String sourceCollection;

  private Configuration configuration;

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
    this.sourceReader = new MigrationSourceReader();
    this.targetWriter = new MigrationTargetWriter();
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
    // Implement the logic to run the migration process
    // This could involve reading data from the source, processing it,
    // and writing it to the target database.
    sourceReader.setup(this.sourceMongoClient, sourceDatabase, sourceCollection);
    targetWriter.setup(this.targetMongoClient, sourceDatabase, sourceCollection);

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

    return this;
  }
}
