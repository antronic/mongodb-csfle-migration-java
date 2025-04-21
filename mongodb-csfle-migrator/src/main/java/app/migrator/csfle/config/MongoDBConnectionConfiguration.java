package app.migrator.csfle.config;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Data;

@Data
public class MongoDBConnectionConfiguration {
  private String uri;
  private boolean tls;
  private String authMechanism = "SCRAM-SHA-256";
  private String authSource = "admin";
  private String tlsTrustStorePath;
  private String tlsTrustStorePassword;
  private String tlsTrustStoreType = "JKS";
  private String tlsKeyStorePath;
  private String tlsKeyStorePassword;
  private String tlsKeyStoreType = "PKCS12";
  // private String tlsInsecure;

  // private String username;
  // private String password;

  public static void validate(MongoDBConnectionConfiguration config, String label) {
    if (config.getUri() == null) {
      throw new IllegalArgumentException(label + ".uri is required");
    }
    if (config.getAuthMechanism() == null) {
      throw new IllegalArgumentException(label + ".authMechanism is required");
    }
    switch (config.getAuthMechanism()) {
      case "SCRAM-SHA-1":
      case "SCRAM-SHA-256":
      case "NONE":
        // No authentication required
        break;
        // if (config.getUsername() == null) {
        //   throw new IllegalArgumentException(label + ".username is required");
        // }
        // if (config.getPassword() == null) {
        //   throw new IllegalArgumentException(label + ".password is required");
        // }
        // break;
      case "MONGODB-X509":
        if (config.getTlsTrustStorePath() == null) {
          throw new IllegalArgumentException(label + ".tlsTrustStorePath is required");
        }
        if (config.getTlsKeyStorePath() == null) {
          throw new IllegalArgumentException(label + ".tlsKeyStorePath is required");
        }
        // if (config.getTlsKeyStorePassword() == null) {
        //   throw new IllegalArgumentException(label + ".tlsCertificateKeyPassword is required");
        // }

        // TODO:
        // Set authSource to "$external" for MONGODB-X509
        config.setAuthSource("$external");
        break;
      default:
        throw new IllegalArgumentException(
            label + ".authMechanism must be one of SCRAM-SHA-1, SCRAM-SHA-256, MONGODB-X509, or NONE");
    }
  }

  public static void mergeConfigurations(Configuration config) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      MongoDBConnectionConfiguration sourceConfig = config.getSourceMongoDB();
      MongoDBConnectionConfiguration targetConfig = config.getTargetMongoDB();

      // Merge source and target configurations
      mapper.updateValue(sourceConfig, targetConfig);
    } catch (JsonMappingException e) {
      throw new RuntimeException("Failed to merge MongoDB configurations", e);
    }
  }
}
