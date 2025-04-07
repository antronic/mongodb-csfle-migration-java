package app.migrator.csfle.archived;

import org.bson.Document;

import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import static com.mongodb.client.model.Filters.eq;

@Deprecated
public class QueryDatabase {
  public static void main(String[] args) {
    String uri = "mongodb://jirachaic:123321@localhost:27088/?directConnection=true";

    ServerApi serverApi = ServerApi.builder()
      .version(ServerApiVersion.V1)
      .build();

    MongoClientSettings settings = MongoClientSettings.builder()
      .applyConnectionString(new ConnectionString(uri))
      .serverApi(serverApi)
      .build();

    try (MongoClient mongoclient = MongoClients.create(settings)) {
      MongoDatabase database = mongoclient.getDatabase("app");
      MongoCollection<Document> people = database.getCollection("people");

      // Query the collection
      Document doc = people.find(eq("name", "Tim")).first();

      if (doc != null) {
        System.out.println("Found a document: " + doc.toJson());
      } else {
        System.out.println("No document found");
      }

      // End the process
      System.out.println("Done");
      System.exit(0);
    }
  }
}
