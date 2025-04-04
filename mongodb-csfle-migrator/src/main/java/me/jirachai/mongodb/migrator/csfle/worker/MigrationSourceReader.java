package me.jirachai.mongodb.migrator.csfle.worker;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import lombok.Setter;

public class MigrationSourceReader {
  private static final Logger logger = LoggerFactory.getLogger(MigrationSourceReader.class);
  private MongoClient mongoClient;
  private String sourceDatabase;
  private String sourceCollection;

  @Setter
  private int skip;
  @Setter
  private int limit;

  public void setup(MongoClient mongoClient, String sourceDatabase, String sourceCollection) {
    this.mongoClient = mongoClient;
    this.sourceDatabase = sourceDatabase;
    this.sourceCollection = sourceCollection;
  }

  // public void read() {
  //   System.out.println("Reading data from source database and collection...");
  //   System.out.println("Target database: " + sourceDatabase + ", collection: " + sourceCollection);
  //   // Implement the logic to read data from the source database and collection
  //   // using the provided MongoClient instance.
  //   // This could involve querying the database, processing the results, etc.
  //   this.mongoClient
  //     .getDatabase(sourceDatabase)
  //     .getCollection(sourceCollection)
  //     .find()
  //       .forEach(doc -> {
  //         // Process each document
  //         System.out.println(doc.toJson());
  //       });
  // }

  public FindIterable<Document> read() {
    // Implement the logic to find all documents in the source collection
    // and return them as a list or stream.
    // This could involve using the MongoDB Java driver to query the collection.

    FindIterable<Document> docs = mongoClient
      .getDatabase(sourceDatabase)
      .getCollection(sourceCollection)
      .find()
        .skip(skip)
        .limit(limit);

    return docs;
  }
}
