package me.jirachai.mongodb.migrator.csfle.config;

import java.util.HashMap;
import java.util.Map;
import org.bson.Document;
import org.bson.types.Binary;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class SchemaConfiguration {
  /**
   * The schema configuration for the MongoDB Encryption Schema
   * @param namespace The namespace of the collection
   * @param schema The encrypt schema of the collection
   */
  private Map</* namespace */ String, CollectionSchema> schemas;

  /**
   * The schema configuration for the MongoDB Encryption Schema
   */
  @Data
  public class CollectionSchema {
    private final String bsonType = "object";
    private EncryptMetadata encryptMetadata;
    private Map<String, Document> properties = new HashMap<>();
  }

  @Data
    public static class EncryptMetadata {
      /**
       * Key ID will retreive by MongoDB Driver
       */
      @JsonProperty("keyId")
      private KeyId[] keyId;
    }

    @Data
    public static class KeyId {
      @JsonProperty("$binary")
      private Binary $binary;
    }

    @Data
    public static class EncryptField {
      private Encrypt encrypt;
    }

    @Data
    public static class Encrypt {
      private String bsonType;
      private String algorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic";
    }
}
