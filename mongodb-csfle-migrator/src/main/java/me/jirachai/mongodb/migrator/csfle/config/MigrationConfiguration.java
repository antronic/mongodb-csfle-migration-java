package me.jirachai.mongodb.migrator.csfle.config;

import java.util.Map;
import lombok.Data;
import java.util.List;

@Data
public class MigrationConfiguration {
  private Map<String, List<String>> targetToMigrate;
  private boolean dropCollectionOnTarget = false;

  private String migrationName = "Migration_" + System.currentTimeMillis();
  private int migrationVersion;
  private String migrationDescription;
}
