package me.jirachai.mongodb.migrator.csfle;

import me.jirachai.mongodb.migrator.csfle.config.Configuration;
import me.jirachai.mongodb.migrator.csfle.service.MongoDBService;
import me.jirachai.mongodb.migrator.csfle.worker.MigrationSourceReader;
import me.jirachai.mongodb.migrator.csfle.worker.MigrationTargetWriter;
import me.jirachai.mongodb.migrator.csfle.worker.MigrationVerifier;
import me.jirachai.mongodb.migrator.csfle.worker.WorkerManager;
import com.mongodb.client.MongoClient;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrationDriver {
    private final Configuration config;
    private final WorkerManager workerManager;
    private final Logger logger = LoggerFactory.getLogger(MigrationDriver.class);
    private MongoDBService sourceService;
    private MongoDBService targetService;
    private Map<String, List<String>> collectionsMap;

    public MigrationDriver(Configuration config) {
        this.config = config;
        this.workerManager = new WorkerManager(
            config.getWorker().getMaxThreads(),
            config.getWorker().getMaxQueueSize()
        );
    }

    public void startMigration() {
        workerManager.initializeWorkers();

        try {
            for (Map.Entry<String, List<String>> entry : collectionsMap.entrySet()) {
                String dbName = entry.getKey();
                List<String> collections = entry.getValue();

                for (String collectionName : collections) {
                    logger.info("Submitting migration task for {}.{}", dbName, collectionName);

                    workerManager.submitTask(collectionName, () -> {
                        MigrationSourceReader reader = new MigrationSourceReader();
                        reader.setup(sourceService.getClient(), dbName, collectionName);
                        reader.read();
                        // Add write and verify logic here
                    });
                }
            }
        } finally {
            shutdown();
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

        this.getCollectionsToMigrate();
    }

    private void shutdown() {
        workerManager.shutdown();
        sourceService.close();
        targetService.close();
    }

    private void dryRun() {
        Configuration _config = Configuration.load("config.json");

        System.out.println(_config.toString());
    }
}
