package app.migrator.csfle.worker;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoSecurityException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;

public class MigrationTargetWriter {
    private static final Logger logger = LoggerFactory.getLogger(MigrationTargetWriter.class);
    private MongoClient mongoClient;
    private String targetDatabase;
    private String targetCollection;
    private final List<Document> failedDocuments = new ArrayList<>();

    public void setup(MongoClient mongoClient, String targetDatabase, String targetCollection) {
        this.mongoClient = mongoClient;
        this.targetDatabase = targetDatabase;
        this.targetCollection = targetCollection;
    }

    public void write(Document document) {
        try {
            this.mongoClient
                .getDatabase(targetDatabase)
                .getCollection(targetCollection)
                .insertOne(document);
        } catch (MongoException e) {
            logger.error("Failed to write document: {}", e.getMessage());
            saveFailedDocument(document, "WRITE_ERROR", e.getMessage());
            throw e;
        } catch (Exception e) {
            logger.error("Unexpected error during document write: {}", e.getMessage());
            saveFailedDocument(document, "UNKNOWN_ERROR", e.getMessage());
            throw new RuntimeException("Write operation failed", e);
        }
    }

    public void writeBatch(List<Document> documents) {
        if (documents == null || documents.isEmpty()) {
            logger.warn("Empty batch received - skipping write operation");
            return;
        }

        logger.info("Attempting to write {} documents to {}.{}",
            documents.size(), targetDatabase, targetCollection);

        try {
            MongoCollection<Document> collection = mongoClient
                .getDatabase(targetDatabase)
                .getCollection(targetCollection);

            InsertManyOptions options = new InsertManyOptions()
                .ordered(false); // Allow unordered inserts
            collection.insertMany(documents, options);

            logger.info("Successfully wrote {} documents to {}.{}",
                documents.size(), targetDatabase, targetCollection);

        } catch (MongoSecurityException e) {
            logger.error("CSFLE encryption failed - check encryption keys and permissions: {}",
                e.getMessage());
            failedDocuments.addAll(documents);
            saveFailedBatch(documents, "CSFLE_ERROR", e.getMessage());
            throw e;

        } catch (MongoBulkWriteException e) {
            logger.error("Bulk write partially failed: {}", e.getMessage());
            handleBulkWriteError(documents, e);

        } catch (MongoException e) {
            logger.error("MongoDB operation failed: {}", e);
            failedDocuments.addAll(documents);
            saveFailedBatch(documents, "WRITE_ERROR", e.getMessage());
            throw e;

        } catch (Exception e) {
            logger.error("Unexpected error during write: {}", e.getMessage());
            failedDocuments.addAll(documents);
            saveFailedBatch(documents, "UNKNOWN_ERROR", e.getMessage());
            throw new RuntimeException("Write operation failed", e);
        }
    }

    private void handleBulkWriteError(List<Document> documents, MongoBulkWriteException e) {
        List<BulkWriteError> errors = e.getWriteErrors();

        for (BulkWriteError error : errors) {
            int index = error.getIndex();
            if (index < documents.size()) {
                Document failedDoc = documents.get(index);
                failedDocuments.add(failedDoc);

                logger.error("[{}.{}] Document write failed at index {}: {}",
                    this.targetDatabase, this.targetCollection,
                    index, error);

                saveFailedDocument(failedDoc, "BULK_WRITE_ERROR", error.getMessage());
            }
        }
    }

    private void saveFailedBatch(List<Document> documents, String errorType, String errorMessage) {
        try {
            MongoCollection<Document> errorCollection = mongoClient
                .getDatabase(targetDatabase)
                .getCollection(targetCollection + "_errors");

            Document errorDoc = new Document()
                .append("timestamp", new java.util.Date())
                .append("errorType", errorType)
                .append("errorMessage", errorMessage)
                .append("collection", targetCollection)
                .append("batchSize", documents.size())
                .append("documents", documents);

            errorCollection.insertOne(errorDoc);

            logger.info("Saved failed batch details to error collection");

        } catch (Exception e) {
            logger.error("Failed to save error details: {}", e.getMessage());
        }
    }

    private void saveFailedDocument(Document document, String errorType, String errorMessage) {
        try {
            MongoCollection<Document> errorCollection = mongoClient
                .getDatabase(targetDatabase)
                .getCollection(targetCollection + "_errors");

            Document errorDoc = new Document()
                .append("timestamp", new java.util.Date())
                .append("errorType", errorType)
                .append("errorMessage", errorMessage)
                .append("collection", targetCollection)
                .append("document", document);

            errorCollection.insertOne(errorDoc);

        } catch (Exception e) {
            logger.error("Failed to save error document: {}", e.getMessage());
        }
    }

    public List<Document> getFailedDocuments() {
        return new ArrayList<>(failedDocuments);
    }

    public void clearFailedDocuments() {
        failedDocuments.clear();
    }
}
