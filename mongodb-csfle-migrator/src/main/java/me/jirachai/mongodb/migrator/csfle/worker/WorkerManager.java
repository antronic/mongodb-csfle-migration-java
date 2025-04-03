package me.jirachai.mongodb.migrator.csfle.worker;

import java.util.concurrent.*;
import java.util.Map;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages worker threads for MongoDB collection migration tasks. Handles task queuing, worker
 * assignment, and execution tracking.
 */
public class WorkerManager {
  private static final Logger logger = LoggerFactory.getLogger(WorkerManager.class);

  private final int maxWorkers;
  private final int queueSize;
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
    this.queueSize = queueSize;
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
  public void submitTask(String collection, Runnable task) {
    try {
      WorkerTask workerTask = new WorkerTask(collection, task);
      taskQueue.offer(workerTask, 1, TimeUnit.SECONDS);
      processQueue();
    } catch (InterruptedException e) {
      logger.error("Failed to submit task for collection {}: {}", collection, e.getMessage());
      Thread.currentThread().interrupt();
    }
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
    return workerStatus.entrySet().stream().filter(entry -> !entry.getValue().isBusy())
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

    executorService.submit(() -> {
      try {
        task.getTask().run();
      } finally {
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
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
