package app.migrator.csfle.worker;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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

  private final AtomicInteger submittedTaskCount = new AtomicInteger(0);
  private final AtomicInteger completedTaskCount = new AtomicInteger(0);
  private final ConcurrentHashMap<String, Boolean> taskTracker = new ConcurrentHashMap<>();

  /**
   * Creates a new WorkerManager with specified capacity.
   *
   * @param maxWorkers maximum number of concurrent workers
   * @param queueSize maximum number of tasks that can be queued
   */
  public WorkerManager(int maxWorkers, int queueSize) {
    this.maxWorkers = maxWorkers;
    this.executorService = Executors.newFixedThreadPool(maxWorkers);
    this.taskQueue = new LinkedBlockingQueue<>(queueSize);
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
    int taskId = submittedTaskCount.incrementAndGet();
    logger.info("TASK_SUBMISSION_START: [{}] Collection: {}, Current queue size: {}",
               taskId, collection, taskQueue.size());
    taskTracker.put(collection, false); // Mark as not completed

    Runnable wrappedTask = () -> {
      try {
        logger.debug("TASK_EXECUTION_START: [{}] Collection: {}", taskId, collection);

        // Execute the user's task with robust error handling
        try {
          task.run();
          logger.debug("TASK_EXECUTION_COMPLETE: [{}] Collection: {}", taskId, collection);
        } catch (OutOfMemoryError oom) {
          logger.error("TASK_OOM_ERROR: [{}] Collection: {} encountered OOM: {}",
                     taskId, collection, oom.getMessage());
          System.gc(); // Request GC
          try {
            Thread.sleep(2000); // Give GC time
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        } catch (Throwable t) {
          logger.error("TASK_EXECUTION_ERROR: [{}] Collection: {} failed with: {}",
                     taskId, collection, t.getMessage(), t);
        }
      } finally {
        // ALWAYS update counters and countdown the latch regardless of success/failure
        completedTaskCount.incrementAndGet();
        taskTracker.put(collection, true); // Mark as completed

        // Log before countdown to avoid race conditions in log output
        logger.debug("TASK_COMPLETING: [{}] Collection: {}, Will countdown latch from: {}",
                   taskId, collection, latch.getCount());

        // Actually countdown the latch
        latch.countDown();

        logger.debug("TASK_LATCH_COUNTDOWN: [{}] Collection: {}, Remaining: {}",
                   taskId, collection, latch.getCount());
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
    //
    while (retryCount < maxRetries) {
    try {
        logger.info("QUEUE_ATTEMPT: [{}] Collection: {}, Attempt: {}/{}, Queue size: {}/{}",
                  taskId, collection, retryCount+1, maxRetries,
                  taskQueue.size(), taskQueue.size() + taskQueue.remainingCapacity());

        boolean offered = taskQueue.offer(workerTask, Integer.MAX_VALUE, TimeUnit.SECONDS);

        if (offered) {
            logger.info("QUEUE_SUCCESS: [{}] Collection: {}, Queue size now: {}",
                      taskId, collection, taskQueue.size());
            processQueue();
            return;
        }

        logger.warn("QUEUE_FULL: [{}] Collection: {}, Retry: {}/{}",
                  taskId, collection, retryCount+1, maxRetries);
        retryCount++;

        // More aggressive retry behavior - exponential backoff
        long currentDelay = retryDelay * (long)Math.pow(2, retryCount-1);
        currentDelay = Math.min(currentDelay, 30000); // Cap at 30 seconds

        if (retryCount < maxRetries) {
            logger.warn("QUEUE_RETRY: [{}] Collection: {}, Waiting: {}ms before retry",
                      taskId, collection, currentDelay);
            Thread.sleep(currentDelay);
        }
    } catch (InterruptedException e) {
        logger.error("TASK_INTERRUPTED: [{}] Collection: {}", taskId, collection, e);
        Thread.currentThread().interrupt();
        throw e; // Re-throw rather than wrapping
    }
}

// If we get here, all retries failed - this is CRITICAL
logger.error("TASK_SUBMISSION_FAILED: [{}] Collection: {} after {} attempts",
            taskId, collection, maxRetries);
throw new RuntimeException("Failed to submit task: " + collection);
  }

  /**
   * Processes queued tasks if workers are available.
   */
  private void processQueue() {
    logger.debug("Processing task queue. Available workers: {}, Queue size: {}",
    getAvailableWorkers(), taskQueue.size());

    String workerId = assignWorker();
    if (getAvailableWorkers() > 0 && !taskQueue.isEmpty() && workerId != null) {
      WorkerTask task = taskQueue.poll();
      logger.debug("Polled task from queue: {}", task);
      if (task != null) {
        logger.debug("Dequeued task for collection: {}", task.getCollection());
        executeTask(workerId, task);
      } else {
        logger.warn("No available task found for worker: {}", workerId);
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
            logger.debug("WORKER_EXECUTING: Worker {} starting task for collection {}",
                         workerId, task.getCollection());
            task.getTask().run();
            logger.debug("WORKER_COMPLETED: Worker {} finished task for collection {}",
                         workerId, task.getCollection());
        } catch (Exception e) {
            logger.error("WORKER_ERROR: Worker {} failed processing collection {}: {}",
                         workerId, task.getCollection(), e.getMessage(), e);
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
    logger.debug("Shutting down worker manager...");
    this.executorService.shutdown();
    try {
      if (!this.executorService.awaitTermination(60, TimeUnit.SECONDS)) {
        logger.warn("Forcing shutdown of executor service...");

        this.executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      this.executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Verifies if all submitted tasks are completed.
   */
  public void verifyAllTasksCompleted() {
    logger.info("TASK_VERIFICATION: Submitted: {}, Completed: {}",
               submittedTaskCount.get(), completedTaskCount.get());

    List<String> incompleteCollections = taskTracker.entrySet().stream()
        .filter(e -> !e.getValue())
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());

    if (!incompleteCollections.isEmpty()) {
        logger.error("INCOMPLETE_TASKS: {} collections were not processed: {}",
                    incompleteCollections.size(), incompleteCollections);
    } else {
        logger.info("ALL_TASKS_COMPLETED: All {} collections were processed successfully",
                   taskTracker.size());
    }
  }
}
