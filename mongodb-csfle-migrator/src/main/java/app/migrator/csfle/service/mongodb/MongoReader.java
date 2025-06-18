package app.migrator.csfle.service.mongodb;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.CountOptions;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(chain = true)
public class MongoReader {
  private static final Logger logger = LoggerFactory.getLogger(MongoReader.class);
  private MongoClient mongoClient;
  @Getter
  private String database;
  @Getter
  private String collection;

  @Setter
  @Getter
  private int skip;
  @Setter
  @Getter
  private int limit;

  public void setup(MongoClient mongoClient, String sourceDatabase, String sourceCollection) {
    this.mongoClient = mongoClient;
    this.database = sourceDatabase;
    this.collection = sourceCollection;
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
    return this.read(new Document());
  }

  public FindIterable<Document> read(Bson filter) {
    // Implement the logic to find all documents in the source collection
    // and return them as a list or stream.
    // This could involve using the MongoDB Java driver to query the collection.

    // logger.info("Skipping {} documents and limiting to {} documents", skip, limit);

    FindIterable<Document> docs = mongoClient
      .getDatabase(database)
      .getCollection(collection)
      .find(filter)
        .skip(skip)
        .sort(new Document("_id", 1))
        .limit(limit);

    return docs;
  }

  public long count(Bson filter) {
    // Set skip and limit options for counting
    CountOptions countOptions = new CountOptions()
      .skip(this.skip)
      .limit(this.limit);

    long count = mongoClient
      .getDatabase(database)
      .getCollection(collection)
      .countDocuments(filter, countOptions);

    // logger.info("Counted {} documents in {}.{} with filter: {}", count, sourceDatabase, sourceCollection, filter);

    return count;
  }
}
