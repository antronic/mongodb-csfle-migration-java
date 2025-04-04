package me.jirachai.mongodb.migrator.csfle.worker;

import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import me.jirachai.mongodb.migrator.csfle.config.Configuration;

public class MigrationManager {
  private final MigrationSourceReader sourceReader;
  private final MigrationTargetWriter targetWriter;

  private final MongoClient sourceMongoClient;
  private final MongoClient targetMongoClient;
  private final String sourceDatabase;
  private final String sourceCollection;

  private Configuration configuration;

  private int batchSize = 1000;
  private int batchCount = 0;
  private long totalCount = 0;
  private int currentBatchSize = 0;
  private int currentBatchCount = 0;
  private int currentBatchIndex = 0;

  public MigrationManager(
      Configuration configuration,
      MongoClient sourceMongoClient,
      MongoClient targetMongoClient,
      String sourceDatabase,
      String sourceCollection) {
    this.configuration = configuration;
    this.sourceReader = new MigrationSourceReader();
    this.targetWriter = new MigrationTargetWriter();

    this.sourceMongoClient = sourceMongoClient;
    this.targetMongoClient = targetMongoClient;
    this.sourceDatabase = sourceDatabase;
    this.sourceCollection = sourceCollection;
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

  public void run() {
    // Implement the logic to run the migration process
    // This could involve reading data from the source, processing it,
    // and writing it to the target database.
    sourceReader.setup(this.sourceMongoClient, sourceDatabase, sourceCollection);
    targetWriter.setup(this.targetMongoClient, sourceDatabase, sourceCollection);

    // Start the migration process
    for (int i = 0; i < batchCount; i++) {
      System.out.println( "Batch: " + i  + " - " + sourceCollection);

      currentBatchIndex = i;
      currentBatchSize = Math.min(batchSize, (int) (totalCount - (i * batchSize)));
      currentBatchCount = Math.min(batchCount, this.getTotalRounds());

      // Read data from the source database and collection
      sourceReader.setSkip(currentBatchIndex * batchSize);
      sourceReader.setLimit(currentBatchSize);
      // Read data from the source
      List<Document> docs = sourceReader.read().into(new ArrayList<>());

      System.out.println("Target database: " + sourceDatabase + ", collection: " + sourceCollection);
      System.out.println("Read " + docs.size() + " documents.");

      // for (Document doc : docs) {
      //   // Process each document
      //   System.out.println(doc.toJson());
      // }

      // Write data to the target
      targetWriter.writeBatch(docs);
    }
  }

  public void initialize() {
    // Initialize the migration process
    // This could involve setting up connections, preparing data structures, etc.
    this.batchSize = configuration.getWorker().getMaxBatchSize();
    this.totalCount = getTotalCountInCollection();
    this.batchCount = getTotalRounds();
  }
}
