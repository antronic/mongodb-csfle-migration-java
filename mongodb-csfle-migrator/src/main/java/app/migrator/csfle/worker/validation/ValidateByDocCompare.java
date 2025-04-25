package app.migrator.csfle.worker.validation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.FindIterable;

import app.migrator.csfle.service.mongodb.MongoReader;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Performs deeper validation by comparing document contents between source and target. This
 * approach verifies that documents were transferred completely and accurately.
 */
@Accessors(chain=true)
public class ValidateByDocCompare {
  private final Logger logger = LoggerFactory.getLogger(ValidateByDocCompare.class);
  private final MongoReader sourceReader;
  private final MongoReader targetReader;

  @Setter
  private int batchSize = 1000;
  @Setter
  private int totalBatch;
  @Setter
  @Getter
  private long totalDocs;

  @Getter
  private boolean isValid = true;

  /**
   * Creates a new document comparison validator.
   *
   * @param sourceReader MongoReader for the source collection
   * @param targetReader MongoReader for the target collection
   *
   */
  public ValidateByDocCompare(MongoReader sourceReader, MongoReader targetReader) {
    this.sourceReader = sourceReader;
    this.targetReader = targetReader;
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

      logger.info("Batch: " + (i + 1) + " - Current batch size: " + currentBatchSize);

      // logger.info("##################");
      // logger.info("{}.{} Accumulated source count: {}", sourceReader.getDatabase(), sourceReader.getCollection(), accumulatedSourceCount);
      // logger.info("{}.{} Accumulated target count: {}", targetReader.getDatabase(), targetReader.getCollection(), accumulatedTargetCount);
      // logger.info("##################");

      processBatch();
    }

    logger.info(
        " --> Finalized batch with {} source documents and {} target documents. | Total batch: {}",
        sourceReader.getDatabase(),
        sourceReader.getCollection(),
        totalBatch
      );
  }

  private void processBatch() {
    Bson filter = new Document(
      "_id",
      new Document()
        .append("$gte", new MinKey())
        .append("$lte", new MaxKey())
    );

    ExecutorService executor = Executors.newFixedThreadPool(2);

    Callable<FindIterable<Document>> sourceDocsTask = () -> {
      logger.info("Reading source documents...");
      FindIterable<Document> docs = sourceReader.read(filter)
        .sort(new Document("_id", 1));

      logger.info("Source documents read successfully.");
      return docs;
    };

    Callable<FindIterable<Document>> targetDocsTask = () -> {
      logger.info("Reading target documents...");
      FindIterable<Document> docs = targetReader.read(filter)
        .sort(new Document("_id", 1));

      logger.info("Target documents read successfully.");
      return docs;
    };

    Future<FindIterable<Document>> sourceDocsFuture = executor.submit(sourceDocsTask);
    Future<FindIterable<Document>> targetDocsFuture = executor.submit(targetDocsTask);


    FindIterable<Document> sourceDocs;
    // Retrieve documents from target collections
    FindIterable<Document> targetDocs;
		try {
			sourceDocs = sourceDocsFuture.get();
      targetDocs = targetDocsFuture.get();

      // Validate documents
      validate(sourceDocs, targetDocs);
		} catch (InterruptedException e) {
      Thread.currentThread().interrupt();
			logger.error(e.getMessage());
		} catch (ExecutionException e) {
      // Handle the exception
			logger.error(e.getMessage());
		} finally {
      executor.shutdown();
    }
  }

  /**
   * Validates document transfer by comparing source and target documents. This method checks for
   * missing or mismatched documents between the two collections.
   */
  private void validate(FindIterable<Document> _sourceDocs, FindIterable<Document> _targetDocs) {
    // Implement logic to read documents from source and target collections
    // For example, using MongoDB Java driver to fetch documents
    List<Document> sourceDocs = _sourceDocs.into(new ArrayList<>()); // Fetch from source
    List<Document> targetDocs = _targetDocs.into(new ArrayList<>()); // Fetch from target

    logger.info("Processed batch with {} source documents and {} target documents.",
                sourceDocs.size(),
                targetDocs.size());

    compareDocs(sourceDocs, targetDocs);
  }

  /**
   * Compares two sets of documents by their _id field and contents. Reports any missing or
   * mismatched documents between source and target.
   *
   * @param sourceDocs List of documents from the source collection
   * @param targetDocs List of documents from the target collection
   */
  private void compareDocs(List<Document> sourceDocs, List<Document> targetDocs) {
    // Create a map of target documents by their _id for efficient lookups
    Map<Object, Document> targetById =
        targetDocs.stream().collect(Collectors.toMap(doc -> doc.get("_id"), Function.identity()));

    // Compare each source document to its corresponding target document
    for (Document src : sourceDocs) {
      Object id = src.get("_id");
      Document tgt = targetById.get(id);

      if (tgt == null) {
        // Document exists in source but not in target
        isValid = false;
        logger.warn("Missing document in target: _id={}", id);
      } else if (!normalize(src).equals(normalize(tgt))) {
        // Documents exist in both, but contents don't match
        isValid = false;
        logger.warn("Mismatch at _id={}\nSRC: {}\nTGT: {}", id, src.toJson(), tgt.toJson());
      } else {
        // Document exists in both and contents match
        logger.info("Document matched: _id={}", id);
      }
    }
  }

  /**
   * Normalizes document representation for consistent comparison. Helps ensure that documents with
   * the same logical content but different representations are properly identified as matching.
   *
   * @param doc Document to normalize
   * @return Normalized string representation of the document
   */
  private String normalize(Document doc) {
    // Normalize by converting to canonical JSON (sorted keys if needed)
    // Optionally: strip metadata or transform to a stable hash
    return doc.toJson(); // or apply your own canonicalizer
  }
}
