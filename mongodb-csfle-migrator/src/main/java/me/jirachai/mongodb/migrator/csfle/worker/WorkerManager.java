package me.jirachai.mongodb.migrator.csfle.worker;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerManager {
    private static final Logger logger = LoggerFactory.getLogger(WorkerManager.class);

    private final int maxWorkers;
    private final int queueSize;
    private final ExecutorService executorService;
    private final BlockingQueue<WorkerTask> taskQueue;
    private final Map<String, WorkerStatus> workerStatus;

    public WorkerManager(int maxWorkers, int queueSize) {
        this.maxWorkers = maxWorkers;
        this.queueSize = queueSize;
        this.executorService = Executors.newFixedThreadPool(maxWorkers);
        this.taskQueue = new ArrayBlockingQueue<>(queueSize);
        this.workerStatus = new ConcurrentHashMap<>();
    }

    @Data
    private static class WorkerStatus {
        private final String workerId;
        private boolean busy;
        private String currentCollection;
        private long processedDocuments;
        private long startTime;
    }

    @Data
    private static class WorkerTask {
        private final String collection;
        private final Runnable task;
    }

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

    private int getAvailableWorkers() {
        return (int) workerStatus.values().stream()
                                .filter(status -> !status.isBusy())
                                .count();
    }

    private String assignWorker() {
        return workerStatus.entrySet().stream()
                          .filter(entry -> !entry.getValue().isBusy())
                          .map(Map.Entry::getKey)
                          .findFirst()
                          .orElse(null);
    }

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

    public void initializeWorkers() {
        for (int i = 0; i < maxWorkers; i++) {
            String workerId = "worker-" + i;
            workerStatus.put(workerId, new WorkerStatus(workerId));
        }
    }

    public Map<String, WorkerStatus> getWorkersStatus() {
        return new ConcurrentHashMap<>(workerStatus);
    }

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
