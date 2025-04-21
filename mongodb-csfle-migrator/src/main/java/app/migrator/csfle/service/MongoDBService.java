package app.migrator.csfle.service;

import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import app.migrator.csfle.config.MongoDBConnectionConfiguration;
import lombok.Getter;
import lombok.Setter;

public class MongoDBService implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(MongoDBService.class);

  @Setter
  @Getter
  private MongoClient client;
  private final String uri;
  private MongoDBConnectionConfiguration configuration;

  public MongoDBService(MongoClient client) {
    this.client = client;
    this.uri = null;
  }

  public MongoDBService(MongoDBConnectionConfiguration configuration) {
    this.configuration = configuration;
    this.uri = this.configuration.getUri();
    this.client = this.setupClient();
  }

  private SSLContext setupSSLContext() {
    try {
      SSLContext sslContext = SSLContextFactory.create(
          configuration.getTlsKeyStorePath(),
          configuration.getTlsKeyStorePassword(),
          configuration.getTlsTrustStorePath(),
          configuration.getTlsTrustStorePassword(),
          configuration.getTlsKeyStoreType(),
          configuration.getTlsTrustStoreType());
      logger.info("SSL context created successfully");

      return sslContext;
    } catch (Exception e) {
      logger.error("Error during SSL context setup: ", e.getMessage());
      throw new RuntimeException("Failed to setup SSL context", e);
    }
  }

  private MongoCredential setupCredential() {
    MongoDBConnectionConfiguration config = this.configuration;
    MongoCredential credential = null;

    if (config.getAuthMechanism() != null) {
      switch (config.getAuthMechanism()) {
        case "SCRAM-SHA-1":
        case "SCRAM-SHA-256":
          // credential = MongoCredential.createScramSha256Credential(
          //     config.getUsername(),
          //     config.getAuthSource(),
          //     config.getPassword().toCharArray());
          break;
        case "MONGODB-X509":
          logger.info("Using MONGODB-X509 authentication mechanism");
          credential = MongoCredential.createMongoX509Credential();
              // .withMechanism(AuthenticationMechanism.MONGODB_X509)
              // .withMechanismProperty("TLS", "true")
              // .withMechanismProperty("TLS_INSECURE", "true")
              // .withMechanismProperty("TLS_TRUST_STORE_PATH", config.getTlsTrustStorePath())
              // .withMechanismProperty("TLS_TRUST_STORE_PASSWORD", config.getTlsTrustStorePassword())
              // .withMechanismProperty("TLS_KEY_STORE_PATH", config.getTlsKeyStorePath())
              // .withMechanismProperty("TLS_KEY_STORE_PASSWORD", config.getTlsKeyStorePassword())
            // .withMechanismProperty("AUTH_SOURCE", config.getAuthSource());
          break;
        default:
          throw new IllegalArgumentException("Unsupported authentication mechanism: " + config.getAuthMechanism());
      }
    }

    return credential;
  }

  private MongoClient setupClient() {
    if (uri == null || uri.isEmpty()) {
      throw new IllegalArgumentException("URI must not be null or empty");
    }

    logger.info("setupCredential().toString()");

    MongoClientSettings.Builder builder = MongoClientSettings.builder()
        .applyConnectionString(new ConnectionString(uri))
        .applyToSslSettings(ssl -> {
          if (configuration.isTls()) {
            ssl.enabled(true);
            ssl.context(setupSSLContext());
            // ssl.invalidHostNameAllowed(true);
          } else {
            ssl.enabled(false);
          }
        })
        .credential(setupCredential())
        .applyToConnectionPoolSettings(settings -> settings
            .maxSize(10)
            .minSize(1));

    try {
      this.client = MongoClients.create(builder.build());
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

    // public MongoClient getClient() {
    //     throw new UnsupportedOperationException("Not supported yet.");
    // }
}
