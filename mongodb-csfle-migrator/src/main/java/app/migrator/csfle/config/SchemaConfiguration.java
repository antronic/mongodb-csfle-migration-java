package app.migrator.csfle.config;

import java.util.HashMap;
import java.util.Map;
import org.bson.BsonDocument;
import org.bson.Document;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
public class SchemaConfiguration {
  /**
   * The schema configuration for the MongoDB Encryption Schema
   * @param namespace The namespace of the collection
   * @param schema The encrypt schema of the collection
   */
  @Setter(AccessLevel.NONE)
  @Getter(AccessLevel.NONE)
  private HashMap</* namespace */ String, BsonDocument> schemas;

  private Map<String, Object> schemasObject;

  // public Document getSchemas() {
  //   ObjectMapper mapper = new ObjectMapper();
  //   Map<String, Object> schemaMap = mapper.convertValue(
  //     schemas,
  //     new TypeReference<Map<String, Object>>() {}
  //   );

  //   return new Document(schemaMap);
  // }

  public HashMap<String, BsonDocument> getSchemas() {
    ObjectMapper mapper = new ObjectMapper();
    HashMap<String, BsonDocument> converted = new HashMap<>();

    for (Map.Entry<String, Object> entry : schemasObject.entrySet()) {
        // Convert Object to Document
        Document document = mapper.convertValue(entry.getValue(), Document.class);

        // Convert Document to BsonDocument
        BsonDocument bsonDocument = BsonDocument.parse(document.toJson());

        converted.put(entry.getKey(), bsonDocument);
    }

    return converted;
  }

  public Document getSchemaAsDocument(String namespace) {
    Object schema = schemas.get(namespace);

    if (schema instanceof Map) {
      // If the schema is a map, convert it to a Document
      ObjectMapper mapper = new ObjectMapper();
      Map<String, Object> schemaMap = mapper.convertValue(
        schema,
        new TypeReference<Map<String, Object>>() {}
      );

      return new Document(namespace, new Document(schemaMap));
    } else {
      throw new IllegalArgumentException("Invalid schema type for namespace: " + namespace);
    }
    // if (schema instanceof String) {
    //   // If the schema is a string, parse it as a JSON string
    //   try {
    //     return mapper.readValue((String) schema, Document.class);
    //   } catch (Exception e) {
    //     throw new IllegalArgumentException("Invalid schema format for namespace: " + namespace, e);
    //   }
    // } else if (schema instanceof Map) {
    //   // If the schema is a map, convert it to a Document
    //   return new Document((Map<String, Object>) schema);
    // } else if (schema instanceof Document) {
    //   // If the schema is already a Document, return it directly
    //   return (Document) schema;
    // }

    // if (schema instanceof Document) {
    //   return (Document) schema;
    // } else if (schema instanceof Binary) {
    //   return Document.parse(((Binary) schema).getData());
    // } else {
    //   throw new IllegalArgumentException("Invalid schema type for namespace: " + namespace);
    // }
  }
}
