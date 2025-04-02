package me.jirachai.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.SSLContext;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;

public class CSFLEInsert {
  //
  // MongoDB connection string
  private static String mongoDBUri = "mongodb://localhost:27019/?directConnection=true";
  private static String databaseName = "app";
  private static String collectionName = "people";


  private MongoClient client;

  public ClientEncryption clientEncryption;
  //
  // KMIP Key vault configuration
  private String keyVaultDb = "encryption";
  private String keyVaultColl = "__keyVault";
  private String keyVaultNamespace = keyVaultDb + "." + keyVaultColl;
  //
  // KMIP provider configuration
  private String kmsProvider = "kmip";
  private String kmsEndpoint = "pykmip.local:5696";
  private Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>();
  private Map<String, Object> providerDetails = new HashMap<>();
  //
  // Setup ClientEncryption
  private void setupClientEncryption() {
    // Create the ClientEncryption object
    //
    Map<String, SSLContext> sslContextMap = new HashMap<>();
    // Set up the SSL context for the KMIP provider
    // sslContextMap.put(kmsProvider, SSLContext.getDefault());

    ClientEncryptionSettings clientEncryptionSettings = ClientEncryptionSettings.builder()
      .keyVaultMongoClientSettings(
        MongoClientSettings.builder()
          .applyConnectionString(new ConnectionString(mongoDBUri))
          .build()
      )
      .keyVaultNamespace(this.keyVaultNamespace)
      .kmsProviders(this.kmsProviders)
      // .kmsProviderSslContextMap(

      // )
      .build();


    this.clientEncryption = ClientEncryptions.create(clientEncryptionSettings);
  }

  private void setupClient() {
    // Create the MongoClient object
    this.client = MongoClients.create(mongoDBUri);
  }

  //
  // Function to generate a new data encryption key (DEK) for the KMIP provider
  private String generateDataKey() {
    BsonBinary datakeyId = this.clientEncryption.createDataKey(kmsProvider, new DataKeyOptions().masterKey(new BsonDocument()));
    String base64DataKeyId = Base64.getEncoder().encodeToString(datakeyId.getData());
    return base64DataKeyId;
  }
  //
  // Function to create the key vault collection and index
  private void createKeyVault() {
    //
    // Determine the index options for the key vault collection
    IndexOptions indexOptions = new IndexOptions()
      .partialFilterExpression(
        new BsonDocument(
          "keyAltName", new BsonDocument("$exists", new BsonBoolean(true))
        )
      ).unique(true);
    //
    MongoDatabase database = this.client.getDatabase(keyVaultDb);
    MongoCollection<Document> collection = database.getCollection(keyVaultColl);
    // Create the key vault index
    collection.createIndex(
      new BsonDocument(
        "keyAltName",
        new BsonInt32(1)
      ),
      indexOptions
    );
  }

  private Map<String, Map<String, Object>> setupKmsProviders() {
    // Set up the KMIP provider configuration
    this.providerDetails.put("endpoint", this.kmsEndpoint);
    this.kmsProviders.put(this.kmsProvider, this.providerDetails);
    return this.kmsProviders;
  }
  //
  //
  // Main function
  public static void main(String[] args) {
    CSFLEInsert csfleInsert = new CSFLEInsert();
    csfleInsert.setupClient();
    Map<String, Map<String, Object>> kmsProviders = csfleInsert.setupKmsProviders();
    csfleInsert.setupClientEncryption();
    csfleInsert.createKeyVault();
    //
    // Client-side field level encryption configuration
    String dekId = csfleInsert.generateDataKey();
    System.out.println("Generated DEK ID: " + dekId);
    //
    // Schema
    Document jsonSchema = new Document()
    .append("bsonType", "object")
    .append("encryptMetadata",
        new Document()
          .append("keyId",
            new ArrayList<>(
              Arrays.asList(
                new Document()
                  .append("$binary",
                    new Document()
                      .append("base64", dekId)
                      .append("subType", "04"))
              )
            )
          )
        )
    .append("properties", new Document()
      .append("ssn", new Document()
        .append("encrypt", new Document()
          .append("bsonType", "string")
          .append("algorithm", "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic")
        )
      )
    );

    HashMap<String, BsonDocument> schemaMap = new HashMap<String, BsonDocument>();
    String namespace = databaseName + "." + collectionName;
    schemaMap.put(namespace, BsonDocument.parse(jsonSchema.toJson()));
    //
    // KMS
    AutoEncryptionSettings autoEncryptionSettings = AutoEncryptionSettings.builder()
        .keyVaultNamespace(csfleInsert.keyVaultNamespace)
        .kmsProviders(kmsProviders)
        .schemaMap(new HashMap<String, BsonDocument>())
        .build();

    MongoClientSettings settings = MongoClientSettings.builder()
        .applyConnectionString(new ConnectionString(mongoDBUri))
        .autoEncryptionSettings(
          autoEncryptionSettings
        )
        .build();

    try (MongoClient mongoclient = MongoClients.create(settings)) {
      MongoDatabase database = mongoclient.getDatabase(databaseName);
      MongoCollection<Document> collection = database.getCollection(collectionName);
      //
      // Create the document
      Document doc = new Document("name", "Tom Jone")
          .append("ssn", "123123123123123");
      //
      // Insert the document
      collection.insertOne(doc);

      System.out.println("Inserted a document: " + doc.toJson());

      // End the process
      System.out.println("Done");
      System.exit(0);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
