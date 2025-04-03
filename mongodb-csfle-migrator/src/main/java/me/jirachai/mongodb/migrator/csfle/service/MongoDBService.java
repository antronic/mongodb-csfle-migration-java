package me.jirachai.mongodb.migrator.csfle.service;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class MongoDBService implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(MongoDBService.class);
  private final MongoClient client;
  private final String uri;

  public MongoDBService(String uri) {
    this.client = setupClient(uri);
    this.uri = uri;
  }

  public static MongoClient setupClient(String uri) {
    try {
      MongoClient client = MongoClients.create(uri);
      logger.info("MongoDB client created successfully");

      return client;
    } catch (Exception e) {
      logger.error("Error during setup: ", e.getMessage());
      throw new RuntimeException("Failed to setup MongoDB client", e);
    }
  }

  public MongoClient getClient() {
    return client;
  }

  public List<String> getAllDatabases() {
    List<String> databases = new ArrayList<String>();

    for (String dbName : client.listDatabaseNames()) {
      databases.add(dbName);
    }

    return databases;
  }

  public List<String> getAllCollections(String dbName) {
    List<String> collections = new ArrayList<String>();

    for (String collectionName : client.getDatabase(dbName).listCollectionNames()) {
      collections.add(collectionName);
    }

    return collections;
  }

  @Override
  public void close() {
    try {
      client.close();
      logger.info("MongoDB client closed successfully");
    } catch (Exception e) {
      logger.error("Error during closing: ", e.getMessage());
    }
  }
}
