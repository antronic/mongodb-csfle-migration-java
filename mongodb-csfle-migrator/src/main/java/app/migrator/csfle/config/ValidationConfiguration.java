package app.migrator.csfle.config;

import java.util.List;
import java.util.Map;

import lombok.Data;

@Data
public class ValidationConfiguration {
  private Map<String, List<String>> targetToValidate;
  private ValidationOptions validationOptions = new ValidationOptions();

  @Data
  public static class ValidationOptions {
    private boolean validateEmptyCollections = true; // Optional, can be set to false if empty collections are acceptable
    // private boolean validateSchema = true;
    // private boolean validateDataIntegrity = true;
    // private boolean validateEncryption = true;
    // private boolean validateIndexes = true;
    // private boolean validatePerformance = false; // Optional, can be set to true if needed
  }
}
