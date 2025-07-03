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
import com.mongodb.client.MongoCursor;

import app.migrator.csfle.common.Constants;
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
  private long totalDocsExamined;

  @Getter
  private boolean isValid = true;

  @Setter
  private String readOperateionType = Constants.ReadOperationType.SKIP;

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
    //
    //
    switch (this.readOperateionType) {
      case Constants.ReadOperationType.CURSOR:
        // Handle cursor-based reading
        logger.info("Using cursor-based reading");
        this.processCursor();
        logger.info("Cursor processing completed");
        break;
      case Constants.ReadOperationType.SKIP:
        // Handle skip-based reading
        this.runSkip();
        break;
      default:
        throw new IllegalArgumentException("Unknown read operation type.");
    }
    //
    // Finalize batch processing
    logger.info(
        " --> Finalized batch with {} source documents and {} target documents. | Total batch: {}",
        sourceReader.getDatabase(),
        sourceReader.getCollection(),
        totalBatch
      );
  }

  private void runCursor() {
    // Implement cursor-based reading logic
  }

  private void runSkip() {
    for (int i = 0; i < totalBatch; i++) {
      int currentBatchSize = Math.min(batchSize, (int) (totalDocs - (i * batchSize)));
      // Set skip and limit for source and target readers
      sourceReader
        .setSkip(batchSize * i)
        .setLimit(currentBatchSize);
      //
      targetReader
        .setSkip(batchSize * i)
        .setLimit(currentBatchSize);

      logger.info("Batch: " + (i + 1) + " - Current batch size: " + currentBatchSize);
      //
      processSkipBatch();
    }
  }

  private void processSkipBatch() {
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

      List<Document> _sourceDocs = sourceDocs.into(new ArrayList<>()); // Fetch from source
      List<Document> _targetDocs = targetDocs.into(new ArrayList<>()); // Fetch from target

      // Validate documents
      validate(_sourceDocs, _targetDocs);
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
  //
  // Process a batch of documents using cursor
  private void processCursor() {
    Bson filter = new Document(
      "_id",
      new Document()
        .append("$gte", new MinKey())
        .append("$lte", new MaxKey())
    );

    ExecutorService executor = Executors.newFixedThreadPool(2);
    // Cursor
    MongoCursor<Document> sourceCursor = sourceReader.readWithCursor(filter);
    MongoCursor<Document> targetCursor = targetReader.readWithCursor(filter);

    try {
        //
        // ==============================================================================
        while (sourceCursor.hasNext() && targetCursor.hasNext()) {
          logger.debug("{}: Source cursor => {}", sourceReader.getNamespace(), sourceCursor.hasNext());
          logger.debug("{}: Target cursor => {}", targetReader.getNamespace(), targetCursor.hasNext());
          //
          // Submit tasks to executor
          Callable<List<Document>> sourceDocsTask = () -> {
            logger.debug("{}: Reading source documents...", sourceReader.getNamespace());
            //
            List<Document> docs = retrieveByCursor(sourceCursor);
            return docs;
          };
          //
          Callable<List<Document>> targetDocsTask = () -> {
            logger.debug("{}: Reading target documents...", targetReader.getNamespace());
            //
            List<Document> docs = retrieveByCursor(targetCursor);
            return docs;
          };
          //
          // ==============================================================================
          //
          // Submit tasks to executor
          Future<List<Document>> sourceDocsFuture = executor.submit(sourceDocsTask);
          Future<List<Document>> targetDocsFuture = executor.submit(targetDocsTask);
          //
          List<Document> sourceDocs;
          List<Document> targetDocs;
          //
          sourceDocs = sourceDocsFuture.get();
          targetDocs = targetDocsFuture.get();
          //
          // Validate documents
          validate(sourceDocs, targetDocs);
        }
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
  //
  // Helper method to retrieve documents from a MongoCursor
  private List<Document> retrieveByCursor(MongoCursor<Document> cursor) {
    List<Document> docs = new ArrayList<>();
    // Keep reading documents while the cursor is valid
    while (isValid && docs.size() < batchSize && cursor.hasNext()) {
      // Process each document as needed
      Document doc = cursor.next();
      docs.add(doc);
    }
    return docs;
  }

  /**
   * Validates document transfer by comparing source and target documents. This method checks for
   * missing or mismatched documents between the two collections.
   */
  private void validate(List<Document> _sourceDocs, List<Document> _targetDocs) {
    // Implement logic to read documents from source and target collections
    // For example, using MongoDB Java driver to fetch documents
    logger.info("{}: Processed batch with {} source documents and {} target documents.",
                sourceReader.getNamespace(),
                _sourceDocs.size(),
                _targetDocs.size()
              );

    compareDocs(_sourceDocs, _targetDocs);
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
      this.totalDocsExamined++;
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
        // logger.debug("Document matched: _id={}", id);
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
