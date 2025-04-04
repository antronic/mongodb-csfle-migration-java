package me.jirachai.mongodb.migrator.csfle;

import me.jirachai.mongodb.migrator.csfle.config.Configuration;
import me.jirachai.mongodb.migrator.csfle.service.MongoCSFLE;
import me.jirachai.mongodb.migrator.csfle.service.MongoDBService;
import me.jirachai.mongodb.migrator.csfle.worker.MigrationManager;
import me.jirachai.mongodb.migrator.csfle.worker.WorkerManager;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mongodb.client.MongoClient;

public class MigrationDriver {
    private final Configuration config;
    private final WorkerManager workerManager;
    private final Logger logger = LoggerFactory.getLogger(MigrationDriver.class);
    private MongoDBService sourceService;
    private MongoDBService targetService;
    // private MongoClient targetMongoClient;
    private Map<String, List<String>> collectionsMap = new HashMap<>();

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
                        MigrationManager migrationManager = new MigrationManager(
                            this.config,
                            sourceService.getClient(),
                            targetService.getClient(),
                            dbName,
                            collectionName
                        );

                        migrationManager.initialize();
                        migrationManager.run();
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

    // private List<String, List<String>> getCollection(String)

    public void setup() {
        // Initialize source and target MongoDB clients
        sourceService = new MongoDBService(config.getSourceMongoDBUri());
        //
        //
        MongoCSFLE csfleClient = new MongoCSFLE(config.getTargetMongoDBUri(), config);
        csfleClient.setup();
        //
        MongoClient targetMongoClient = csfleClient.getMongoClient();
        targetService = new MongoDBService(targetMongoClient);
        //
        //
        // this.getCollectionsToMigrate();
        Map<String, List<String>> dbs = this.config.getMigrateTarget();

        if (dbs != null) {
            for (Map.Entry<String, List<String>> entry : dbs.entrySet()) {
                String dbName = entry.getKey();
                List<String> collections = entry.getValue();

                if (collections != null && !collections.isEmpty()) {
                    this.collectionsMap.put(dbName, collections);
                }
            }
        } else {
            this.getCollectionsToMigrate();
        }
    }

    private void shutdown() {
        workerManager.shutdown();
        sourceService.close();
        targetService.close();
    }

    // private void dryRun() {
    //     Configuration _config = Configuration.load("config.json");

    //     System.out.println(_config.toString());
    // }
}
