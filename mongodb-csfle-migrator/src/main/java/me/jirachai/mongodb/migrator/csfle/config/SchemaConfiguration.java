package me.jirachai.mongodb.migrator.csfle.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.bson.types.Binary;
import lombok.Data;

@Data
public class SchemaConfiguration {
  /**
   * The schema configuration for the MongoDB Encryption Schema
   * @param namespace The namespace of the collection
   * @param schema The encrypt schema of the collection
   */
  private Map</* namespace */ String, /* schema */ SchemaConfiguration.Schema> schemas = new HashMap<>();
  /**
   * The schema configuration for the MongoDB Encryption Schema
   */
  @Data
  public class Schema {
    private String namespace;
    private final String bsonType = "object";
    private EncryptMetadata encryptMetadata;
    private Map<String, Document> properties = new HashMap<String, Document>();
  }

  @Data
    public static class EncryptMetadata {
      private List<KeyId> keyId = new ArrayList<>();
    }

    @Data
    public static class KeyId {
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
