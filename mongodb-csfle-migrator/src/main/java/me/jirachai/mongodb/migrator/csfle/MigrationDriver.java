package me.jirachai.mongodb.migrator.csfle;

import me.jirachai.mongodb.migrator.csfle.config.Configuration;
import me.jirachai.mongodb.migrator.csfle.worker.MigrationSourceReader;
import me.jirachai.mongodb.migrator.csfle.worker.MigrationTargetWriter;
import me.jirachai.mongodb.migrator.csfle.worker.MigrationVerifier;
import com.mongodb.client.MongoClient;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.List;
import java.util.ArrayList;
import org.bson.Document;

public class MigrationDriver {
    private final Configuration config;
    private final MongoClient sourceClient;
    private final MongoClient targetClient;
    private final ExecutorService executorService;
    private final List<MigrationSourceReader> readers;
    private final List<MigrationTargetWriter> writers;
    private final MigrationVerifier verifier;

    public MigrationDriver(Configuration config, MongoClient sourceClient, MongoClient targetClient) {
        this.config = config;
        this.sourceClient = sourceClient;
        this.targetClient = targetClient;
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
            // For each collection that matches the prefix filter
            for (String collectionName : getCollectionsToMigrate()) {
                // Assign read tasks
                readers.forEach(reader -> {
                    executorService.submit(() -> {
                        // List<Document> documents = reader.read(sourceClient, collectionName, config.getWorker().getReadLimit());
                        // Pass documents to writer
                        // writers.get(0).write(targetClient, collectionName, documents);
                        // Verify migration
                        // verifier.verify(sourceClient, targetClient, collectionName, documents);
                    });
                });
            }
        } finally {
            shutdown();
        }
    }

    private List<String> getCollectionsToMigrate() {
        // Implementation to get collections based on prefix filter
        return new ArrayList<>();
    }

    private void shutdown() {
        executorService.shutdown();
        sourceClient.close();
        targetClient.close();
    }

    private void dryRun() {
        Configuration _config = Configuration.load("config.json");

        System.out.println(_config.toString());
    }
}