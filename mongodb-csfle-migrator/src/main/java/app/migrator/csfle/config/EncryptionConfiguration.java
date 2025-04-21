package app.migrator.csfle.config;

import java.util.HashMap;
import java.util.Map;

import lombok.Data;

@Data
public class EncryptionConfiguration {
  private String keyVaultDb = "encryption";
  private String keyVaultColl = "__keyVault";

  private String kmsProvider = "local";
  private String cryptSharedLibPath;
  private Map<String, Object> extraOptions = new HashMap<>();
  //
  // KMIP provider configuration
  //
  // Provider: Local
  private String masterKeyFilePath;
  //
  // Provider: KMIP
  private String kmsEndpoint;
  private String keyStorePath;
  private String keyStorePassword;
  private String trustStorePath;
  private String trustStorePassword;
  private String keyStoreType;
  private String trustStoreType;
  private String keyVaultNamespace;

  public static void validate(EncryptionConfiguration config) {

    if (config.getKmsProvider().equals("local") && config.getMasterKeyFilePath() == null) {
      throw new IllegalArgumentException("encryption.masterKeyFilePath is required");
    }

    // Validate kmsProvider
    if (config.getKmsProvider().equals("kmip")) {

      if (config.getKmsEndpoint() == null) {
        throw new IllegalArgumentException("encryption.kmsEndpoint is required");
      }
      // Validate keyStore and trustStore
      if (config.getKeyStorePath() == null) {
        throw new IllegalArgumentException("encryption.keyStorePath is required");
      }
      if (config.getKeyStorePassword() == null) {
        throw new IllegalArgumentException("encryption.keyStorePassword is required");
      }
      if (config.getKeyStoreType() == null) {
        throw new IllegalArgumentException("encryption.keyStoreType is required");
      }
      if (config.getTrustStorePath() == null) {
        throw new IllegalArgumentException("encryption.trustStorePath is required");
      }
      if (config.getTrustStorePassword() == null) {
        throw new IllegalArgumentException("encryption.trustStorePassword is required");
      }
      if (config.getTrustStoreType() == null) {
        throw new IllegalArgumentException("encryption.trustStoreType is required");
      }
    }

    // Validate cryptSharedLibPath
    if (config.getCryptSharedLibPath() == null) {
      throw new IllegalArgumentException("encryption.cryptSharedLibPath is required");
    }

    // if (config.getKeyVaultDb() == null) {
    // throw new IllegalArgumentException("encryption.keyVaultDb is required");
    // }
    // if (config.getKeyVaultColl() == null) {
    // throw new IllegalArgumentException("encryption.keyVaultColl is required");
    // }
  }
}