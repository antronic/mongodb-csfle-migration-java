package app.migrator.csfle.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import app.migrator.csfle.config.Configuration;
import app.migrator.csfle.config.MongoDBConnectionConfiguration;
import lombok.Getter;
import lombok.Setter;

public class MongoDBService implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(MongoDBService.class);

  @Setter
  @Getter
  private MongoClient client;
  private final String mongoUri;
  private final Configuration configuration;
  private final MongoDBConnectionConfiguration connectionConfiguration;
  @Getter
  private MongoClientSettings.Builder mongoClientSettingsBuilder = MongoClientSettings.builder();

  public MongoDBService(Configuration configuration, MongoDBConnectionConfiguration connectionConfiguration) {
    this.configuration = configuration;
    // Set the connection configuration
    this.connectionConfiguration = connectionConfiguration;
    this.mongoUri = this.connectionConfiguration.getUri();
  }

  public MongoDBService(Configuration configuration, MongoDBConnectionConfiguration connectionConfiguration, MongoClientSettings.Builder mongoClientSettingsBuilder) {
    this.configuration = configuration;
    // Set the connection configuration
    this.connectionConfiguration = connectionConfiguration;
    this.mongoUri = this.connectionConfiguration.getUri();
    this.mongoClientSettingsBuilder = mongoClientSettingsBuilder;
  }

  private SSLContext setupSSLContext() {
    try {
      SSLContext sslContext = SSLContextFactory.create(
          this.connectionConfiguration.getTlsKeyStorePath(),
          this.connectionConfiguration.getTlsKeyStorePassword(),
          this.connectionConfiguration.getTlsTrustStorePath(),
          this.connectionConfiguration.getTlsTrustStorePassword(),
          this.connectionConfiguration.getTlsKeyStoreType(),
          this.connectionConfiguration.getTlsTrustStoreType());
      logger.info("SSL context created successfully");

      return sslContext;
    } catch (Exception e) {
      logger.error("Error during SSL context setup: ", e.getMessage());
      throw new RuntimeException("Failed to setup SSL context", e);
    }
  }

  private MongoCredential setupCredential() {
    MongoDBConnectionConfiguration config = this.connectionConfiguration;
    MongoCredential credential = null;

    if (config.getAuthMechanism() != null) {
      switch (config.getAuthMechanism()) {
        case "SCRAM-SHA-1":
        case "SCRAM-SHA-256":
          break;
        case "MONGODB-X509":
          logger.info("Using MONGODB-X509 authentication mechanism");
          credential = MongoCredential.createMongoX509Credential();
          break;
        default:
          throw new IllegalArgumentException("Unsupported authentication mechanism: " + config.getAuthMechanism());
      }
    }

    return credential;
  }

  public MongoClient setup() {
    if (this.mongoUri == null || this.mongoUri.isEmpty()) {
      throw new IllegalArgumentException("URI must not be null or empty");
    }
    //
    // Setup MongoClientSettings
    this.mongoClientSettingsBuilder
        .applicationName("MongoDB CSFLE Migrator - Service")
        .applyConnectionString(new ConnectionString(this.mongoUri))
        .applyToClusterSettings(settings -> settings
            .applyConnectionString(new ConnectionString(this.mongoUri))
            .serverSelectionTimeout(this.configuration.getWorker().getServerSelectionTimeoutMs(), TimeUnit.MILLISECONDS)
        )
        .applyToSslSettings(ssl -> {
          if (this.connectionConfiguration.isTls()) {
            ssl.enabled(true);
            ssl.context(setupSSLContext());
          } else {
            ssl.enabled(false);
          }
        })
        .applyToConnectionPoolSettings(
            builder -> builder
              .minSize(0)
              .maxSize(10)
              .maxWaitTime(this.configuration.getWorker().getMaxWaitTimeMs(), TimeUnit.MILLISECONDS)
            )
        .applyToSocketSettings(settings -> settings
            .connectTimeout(this.configuration.getWorker().getSocketConnectionTimeoutMs(), TimeUnit.MILLISECONDS)
            .readTimeout(this.configuration.getWorker().getSocketReadTimeoutMs(), TimeUnit.MILLISECONDS)
          )
        .retryWrites(true)
        .retryReads(true);
    //
    // Set credential if authentication is required
    if (this.connectionConfiguration.getAuthMechanism() != null) {
      MongoCredential credential = setupCredential();
      if (credential != null) {
        this.mongoClientSettingsBuilder.credential(credential);
      }
    }
    //
    // Create the MongoClient
    try {
      this.client = MongoClients.create(this.mongoClientSettingsBuilder.build());
      logger.info("MongoDB client created successfully");

      return client;
    } catch (Exception e) {
      logger.error("Error during setup: ", e.getMessage());
      throw new RuntimeException("Failed to setup MongoDB client", e);
    }
  }

  public List<String> getAllDatabases() {
    List<String> databases = new ArrayList<>();

    for (String dbName : client.listDatabaseNames()) {
      databases.add(dbName);
    }

    return databases;
  }

  public List<String> getAllCollections(String dbName) {
    List<String> collections = new ArrayList<>();

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
