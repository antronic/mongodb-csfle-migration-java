package me.jirachai.mongodb.migrator.csfle;

import me.jirachai.mongodb.migrator.csfle.config.Configuration;
import me.jirachai.mongodb.migrator.csfle.service.MongoDBService;
import me.jirachai.mongodb.migrator.csfle.worker.MigrationSourceReader;
import me.jirachai.mongodb.migrator.csfle.worker.MigrationTargetWriter;
import me.jirachai.mongodb.migrator.csfle.worker.MigrationVerifier;
import com.mongodb.client.MongoClient;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import org.bson.Document;

public class MigrationDriver {
  private final Configuration config;
  private final ExecutorService executorService;
  private final List<MigrationSourceReader> readers;
  private final List<MigrationTargetWriter> writers;
  private final MigrationVerifier verifier;

  private  MongoDBService sourceService;
  private  MongoDBService targetService;
  private Map<String, List<String>> collectionsMap;

  public MigrationDriver(Configuration config) {
    this.config = config;
    this.executorService = Executors.newFixedThreadPool(config.getWorker().getMaxThreads());
    this.readers = new ArrayList<>();
    this.writers = new ArrayList<>();
    this.verifier = new MigrationVerifier();
  }

  public void startMigration() {
    // Initialize workers based on configuration
    for (int i = 0; i < config.getWorker().getMaxThreads(); i++) {
      readers.add(new MigrationSourceReader());
      writers.add(new MigrationTargetWriter());
    }

    // Start the migration process
    try {
      for (Map.Entry<String, List<String>> entry: collectionsMap.entrySet()) {
        String dbName = entry.getKey();
        List<String> collections = entry.getValue();

        System.out.println("Database: " + dbName);
        System.out.println("Collections: " + collections);

        // For each collection in the database
        for (String collectionName : collections) {
          System.out.println("Migrating collection: " + collectionName);

          // MigrationSourceReader reader = new MigrationSourceReader();
          // reader.setup(sourceService.getClient(), dbName, collectionName);
          // reader.read();
          // Assign read tasks
          readers.forEach(reader -> {
            executorService.submit(() -> {
              reader.setup(sourceService.getClient(), dbName, collectionName);
              reader.read();
            });
          });
        }
      }

      // For each collection that matches the prefix filter
      // for (String collectionName : getCollectionsToMigrate()) {
      //   // Assign read tasks
      //   readers.forEach(reader -> {
      //     executorService.submit(() -> {
      //       // List<Document> documents = reader.read(sourceClient, collectionName,
      //       // config.getWorker().getReadLimit());
      //       // Pass documents to writer
      //       // writers.get(0).write(targetClient, collectionName, documents);
      //       // Verify migration
      //       // verifier.verify(sourceClient, targetClient, collectionName, documents);
      //     });
      //   });
      // }
    } finally {
      // shutdown();
    }
  }

  private void getCollectionsToMigrate() {
    // Implementation to get collections based on prefix filter
    List<String> sourceDatabases = Arrays.asList(config.getSourceDatabases());

    Map<String, List<String>> _collectionsMap = new HashMap<>();

    for (String dbName : sourceDatabases) {
      // Get all collections in the database
      List<String> collections = sourceService.getAllCollections(dbName);
      _collectionsMap.put(dbName, collections);
      // for (String collection : collections) {
        // Check if collection matches prefix filter
        // if (collection.startsWith(config.getCollectionPrefix())) {
        //   List<String> result = new ArrayList<>();
        //   result.add(collection);
        //   return result;
        // }
      // }
    }

    this.collectionsMap = _collectionsMap;
  }

  private List<String> getCollectionsToMigrateOfDatabaseList(String dbName) {
    return this.collectionsMap.get(dbName);
  }

  public void setup() {
    // Initialize source and target MongoDB clients
    sourceService = new MongoDBService(config.getSourceMongoDBUri());
    targetService = new MongoDBService(config.getTargetMongoDBUri());

    // Initialize CSFLE manager
    // CSFLEManager csfleManager = new CSFLEManager(config.getEncryption());
    this.getCollectionsToMigrate();
  }

  private void shutdown() {
    executorService.shutdown();
    sourceService.close();
    targetService.close();
  }

  private void dryRun() {
    Configuration _config = Configuration.load("config.json");

    System.out.println(_config.toString());
  }
}
