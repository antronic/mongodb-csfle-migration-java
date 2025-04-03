package me.jirachai.mongodb.migrator.csfle.worker;

import org.bson.Document;
import com.mongodb.client.MongoClient;

public class MigrationSourceReader {
  private MongoClient mongoClient;
  private String sourceDatabase;
  private String sourceCollection;

  public void setup(MongoClient mongoClient, String sourceDatabase, String sourceCollection) {
    this.mongoClient = mongoClient;
    this.sourceDatabase = sourceDatabase;
    this.sourceCollection = sourceCollection;
  }

  public void read() {
    System.out.println("Reading data from source database and collection...");
    System.out.println("Target database: " + sourceDatabase + ", collection: " + sourceCollection);
    // Implement the logic to read data from the source database and collection
    // using the provided MongoClient instance.
    // This could involve querying the database, processing the results, etc.
    this.mongoClient
      .getDatabase(sourceDatabase)
      .getCollection(sourceCollection)
      .find()
        .forEach(doc -> {
          // Process each document
          System.out.println(doc.toJson());
        });
  }
}
