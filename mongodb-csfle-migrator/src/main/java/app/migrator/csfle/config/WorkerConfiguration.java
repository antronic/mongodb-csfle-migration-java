package app.migrator.csfle.config;

import java.util.Arrays;

import app.migrator.csfle.common.Constants;
import lombok.Data;

@Data
public class WorkerConfiguration {
  private int maxThreads = 10;
  private int maxQueueSize = 1000;
  private int maxBatchSize = 100;
  private int maxBatchWaitTime = Integer.MAX_VALUE; // in millisecondsprivate int maxBatchSize = 100;
  private int retryDelay = 1000; // in milliseconds
  private boolean enableLogging = true;
  //
  // Read configuration
  // Type of read operation (cursor or skip)
  private String readOperationType = Constants.ReadOperationType.CURSOR;

  public static void validate(WorkerConfiguration config) {
    // Read operation type, can be "cursor" or "skip"
    if (!Arrays.asList(Constants.ReadOperationType.CURSOR, Constants.ReadOperationType.SKIP).contains(config.getReadOperationType())) {
      throw new IllegalArgumentException("Invalid readOperationType: " + config.getReadOperationType());
    }
    // if (config.getMaxThreads() <= 0) {
    //   throw new IllegalArgumentException("Invalid maxThreads: " + config.getMaxThreads());
    // }
    // if (config.getMaxQueueSize() <= 0) {
    //   throw new IllegalArgumentException("Invalid maxQueueSize: " + config.getMaxQueueSize());
    // }
    // if (config.getMaxBatchSize() <= 0) {
    //   throw new IllegalArgumentException("Invalid maxBatchSize: " + config.getMaxBatchSize());
    // }
    // if (config.getMaxBatchWaitTime() < 0) {
    //   throw new IllegalArgumentException("Invalid maxBatchWaitTime: " + config.getMaxBatchWaitTime());
    // }
    // if (config.getRetryDelay() < 0) {
    //   throw new IllegalArgumentException("Invalid retryDelay: " + config.getRetryDelay());
    // }
  }
}
