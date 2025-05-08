package app.migrator.csfle.worker;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Data;

/**
 * Manages worker threads for MongoDB collection migration tasks. Handles task queuing, worker
 * assignment, and execution tracking.
 */
public class WorkerManager {
  private static final Logger logger = LoggerFactory.getLogger(WorkerManager.class);

  private final int maxWorkers;
  private final ExecutorService executorService;
  private final BlockingQueue<WorkerTask> taskQueue;
  private final Map<String, WorkerStatus> workerStatus;

  /**
   * Creates a new WorkerManager with specified capacity.
   *
   * @param maxWorkers maximum number of concurrent workers
   * @param queueSize maximum number of tasks that can be queued
   */
  public WorkerManager(int maxWorkers, int queueSize) {
    this.maxWorkers = maxWorkers;
    this.executorService = Executors.newFixedThreadPool(maxWorkers);
    this.taskQueue = new ArrayBlockingQueue<>(queueSize);
    this.workerStatus = new ConcurrentHashMap<>();
  }

  /**
   * Represents the current status of a worker thread.
   */
  @Data
  private static class WorkerStatus {
    private final String workerId;
    private boolean busy;
    private String currentCollection;
    private long processedDocuments;
    private long startTime;
  }

  /**
   * Represents a migration task for a specific collection.
   */
  @Data
  private static class WorkerTask {
    private final String collection;
    private final Runnable task;
  }

  /**
   * Submits a new migration task for processing.
   *
   * @param collection name of the collection to migrate
   * @param task runnable containing the migration logic
   * @throws InterruptedException if the task submission is interrupted
   */
  public void submitTask(String collection, Runnable task, CountDownLatch latch) throws InterruptedException {
    Runnable wrappedTask = () -> {
      try {
        task.run();
      } finally {
        latch.countDown();
      }
    };

    WorkerTask workerTask = new WorkerTask(collection, wrappedTask);
    int retryCount = 0;
    int maxRetries = Integer.MAX_VALUE; // Configurable
    long retryDelay = 1000; // 1 second delay between retries

    logger.info("Submitting task for collection: " + collection);
    //
    // Retry logic for task submission
    // This is a simple retry mechanism. In a real-world scenario, you might want to use
    // while (retryCount < maxRetries) {
    //
    // Retry until the task is successfully added to the queue or interrupted
    while (!Thread.currentThread().isInterrupted()) {
      try {
        logger.debug("Attempting to submit task for collection: {}", collection);
        boolean offered = taskQueue.offer(workerTask, Integer.MAX_VALUE, TimeUnit.HOURS);
        if (offered) {
          logger.debug("Task submitted for collection: {}", collection);
          processQueue();
          return;
        }

        retryCount++;
        if (retryCount < maxRetries) {
          logger.warn("Queue full for collection {}, retry {}/{} after {} ms", collection,
              retryCount, maxRetries, retryDelay);
          Thread.sleep(retryDelay);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Task submission interrupted for collection: {}", collection);
        throw new RuntimeException("Task submission interrupted for collection: " + collection, e);
      }
    }

    // If we get here, all retries failed
    throw new RuntimeException(
        String.format("Failed to submit task for collection %s after %d retries - queue full",
            collection, maxRetries));
  }

  /**
   * Processes queued tasks if workers are available.
   */
  private void processQueue() {
    if (getAvailableWorkers() > 0 && !taskQueue.isEmpty()) {
      WorkerTask task = taskQueue.poll();
      if (task != null) {
        String workerId = assignWorker();
        if (workerId != null) {
          executeTask(workerId, task);
        }
      }
    }
  }

  /**
   * Gets the number of available (non-busy) workers.
   *
   * @return count of available workers
   */
  private int getAvailableWorkers() {
    return (int) workerStatus.values().stream().filter(status -> !status.isBusy()).count();
  }

  /**
   * Assigns an available worker for task execution.
   *
   * @return workerId of the assigned worker, or null if none available
   */
  private String assignWorker() {
    return workerStatus.entrySet().stream()
      .filter(entry -> !entry.getValue().isBusy())
      .map(Map.Entry::getKey).findFirst().orElse(null);
  }

  /**
   * Executes a task using the assigned worker.
   *
   * @param workerId ID of the worker to execute the task
   * @param task task to be executed
   */
  private void executeTask(String workerId, WorkerTask task) {
    WorkerStatus status = workerStatus.get(workerId);
    status.setBusy(true);
    status.setCurrentCollection(task.getCollection());
    status.setStartTime(System.currentTimeMillis());

    this.executorService.submit(() -> {
      try {
        task.getTask().run();
      } finally {
        logger.info("Task completed for collection: {}", task.getCollection());
        status.setBusy(false);
        status.setCurrentCollection(null);
        status.setProcessedDocuments(status.getProcessedDocuments() + 1);
        processQueue(); // Process next task if available
      }
    });
  }

  /**
   * Initializes worker threads and their status tracking.
   */
  public void initializeWorkers() {
    for (int i = 0; i < maxWorkers; i++) {
      String workerId = "worker-" + i;
      workerStatus.put(workerId, new WorkerStatus(workerId));
    }
  }

  /**
   * Gets the current status of all workers.
   *
   * @return map of worker IDs to their current status
   */
  public Map<String, WorkerStatus> getWorkersStatus() {
    return new ConcurrentHashMap<>(workerStatus);
  }

  /**
   * Shuts down the worker manager and its executor service. Waits for tasks to complete or forces
   * shutdown after timeout.
   */
  public void shutdown() {
    this.executorService.shutdown();
    try {
      if (!this.executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS)) {
        logger.warn("Forcing shutdown of executor service...");

        this.executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      this.executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  public void awaitTermination() {
    try {
      this.shutdown();
      this.executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
