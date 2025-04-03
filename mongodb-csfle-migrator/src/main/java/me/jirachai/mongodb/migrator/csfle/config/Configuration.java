package me.jirachai.mongodb.migrator.csfle.config;

import lombok.Data;

@Data
public class Configuration {
  private String sourceMongoDBUri;
  private String targetMongoDBUri;
  private String sourceDatabase;
  private String targetDatabase;

  private WorkerConfig worker = new WorkerConfig();

  @Data
  public static class WorkerConfig {
    private int maxThreads = 10;
    private int maxQueueSize = 1000;
    private int maxBatchSize = 100;
    private int maxBatchWaitTime = 1000; // in milliseconds
    private int maxRetryCount = 3;
    private int retryDelay = 1000; // in milliseconds
    private boolean enableLogging = true;
  }
}
