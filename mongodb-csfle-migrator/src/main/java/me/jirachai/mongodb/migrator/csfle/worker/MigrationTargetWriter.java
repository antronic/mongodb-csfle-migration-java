package me.jirachai.mongodb.migrator.csfle.worker;

import java.util.List;
import org.bson.Document;
import com.mongodb.client.MongoClient;

public class MigrationTargetWriter {
  private MongoClient mongoClient;
  private String targetDatabase;
  private String targetCollection;

  public void setup(MongoClient mongoClient, String targetDatabase, String targetCollection) {
    this.mongoClient = mongoClient;
    this.targetDatabase = targetDatabase;
    this.targetCollection = targetCollection;
  }

  public void write(Document document) {
    // Implement the logic to write the document to the target database and collection
    // using the provided MongoClient instance.
    // This could involve inserting the document, updating it, etc.
    this.mongoClient
        .getDatabase(targetDatabase)
        .getCollection(targetCollection)
        .insertOne(document);
  }

  public void writeBatch(List<Document> documents) {
    // Implement the logic to write a batch of documents to the target database and collection
    // using the provided MongoClient instance.
    // This could involve inserting multiple documents in a single operation.
    this.mongoClient
        .getDatabase(targetDatabase)
        .getCollection(targetCollection)
        .insertMany(documents);
  }
}
