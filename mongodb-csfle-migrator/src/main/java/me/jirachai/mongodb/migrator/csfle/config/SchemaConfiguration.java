package me.jirachai.mongodb.migrator.csfle.config;

import java.util.Map;
import org.bson.Document;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

@Data
public class SchemaConfiguration {
  /**
   * The schema configuration for the MongoDB Encryption Schema
   * @param namespace The namespace of the collection
   * @param schema The encrypt schema of the collection
   */
  private Map</* namespace */ String, Object> schemas;

  public Object getSchema(String namespace) {
    return schemas.get(namespace);
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
