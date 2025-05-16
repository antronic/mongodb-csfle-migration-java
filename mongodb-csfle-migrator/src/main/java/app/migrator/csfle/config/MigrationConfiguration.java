package app.migrator.csfle.config;

import java.util.List;
import java.util.Map;

import lombok.Data;

@Data
public class MigrationConfiguration {
  private Map<String, List<String>> targetToMigrate;
  private MigrationOptions migrationOptions = new MigrationOptions();

  @Data
  public static class MigrationOptions {
    private boolean createCollectionEvenEmpty = true;
    // private boolean dropCollectionOnTarget = false;
    // private String migrationName = "Migration_" + System.currentTimeMillis();
    // private int migrationVersion;
    // private String migrationDescription;
  }
}
