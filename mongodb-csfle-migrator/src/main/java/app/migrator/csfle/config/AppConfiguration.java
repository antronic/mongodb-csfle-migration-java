package app.migrator.csfle.config;

import lombok.Data;

@Data
public class AppConfiguration {
  private LogOptions logOptions = new LogOptions();
  @Data
  public static class LogOptions {
    private String logLevel = "INFO";
  }
}
