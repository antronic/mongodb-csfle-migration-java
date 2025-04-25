package app.migrator.csfle.config;

import java.util.List;
import java.util.Map;

import lombok.Data;

@Data
public class ValidationConfiguration {
  private Map<String, List<String>> targetToValidate;
  private boolean dropCollectionOnTarget = false;

  private String migrationName = "Migration_" + System.currentTimeMillis();
  private int migrationVersion;
  private String migrationDescription;
}
