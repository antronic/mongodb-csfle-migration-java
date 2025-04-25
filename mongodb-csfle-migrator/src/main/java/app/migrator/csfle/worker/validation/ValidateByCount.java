package app.migrator.csfle.worker.validation;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.migrator.csfle.service.mongodb.MongoReader;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(chain = true)
public class ValidateByCount {
  private final Logger logger = LoggerFactory.getLogger(ValidateByCount.class);
  private final MongoReader sourceReader;
  private final MongoReader targetReader;

  @Setter
  private int batchSize = 1000;
  @Setter
  private int totalBatch;
  @Setter
  private long totalDocs;

  @Getter
  private long accumulatedSourceCount;
  @Getter
  private long accumulatedTargetCount;

  public ValidateByCount(MongoReader sourceReader, MongoReader targetReader) {
    this.sourceReader = sourceReader;
    this.targetReader = targetReader;
  }

  public void count() {
    Bson filter = new Document(
      "_id",
      new Document()
        .append("$gte", new MinKey())
        .append("$lte", new MaxKey())
    );

    // Count documents in the source and target collections
    accumulatedSourceCount += sourceReader.count(filter);
    accumulatedTargetCount += targetReader.count(filter);
  }

  public void run() {
    for (int i = 0; i < totalBatch; i++) {
      int currentBatchSize = Math.min(batchSize, (int) (totalDocs - (i * batchSize)));

      sourceReader
        .setSkip(batchSize * i)
        .setLimit(currentBatchSize);

      targetReader
        .setSkip(batchSize * i)
        .setLimit(currentBatchSize);

      logger.info("Batch: " + i + " - Current batch size: " + currentBatchSize);

      this.count();

      logger.info("##################");
      logger.info("{}.{} Accumulated source count: {}", sourceReader.getDatabase(), sourceReader.getCollection(), accumulatedSourceCount);
      logger.info("{}.{} Accumulated target count: {}", targetReader.getDatabase(), targetReader.getCollection(), accumulatedTargetCount);
      logger.info("##################");
    }
  }
}
