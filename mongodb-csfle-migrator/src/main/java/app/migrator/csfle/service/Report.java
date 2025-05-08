package app.migrator.csfle.service;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVWriter;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(chain = true)
public class Report {
  private static final Logger logger = LoggerFactory.getLogger(Report.class);
  private final String name;

  @Setter
  public String[] headers;
  @Setter
  public List<String[]> data = new ArrayList<>();

  public Report(String reportName) {
    // Initialize the report with the given name
    this.name = reportName;
  }

  public void generate() throws IOException {
    // Generate the report
    logger.info("Generating report: " + name);
    // Add logic to generate the report
    createDirectory();
    //
    // Create a CSV file with the report name
    String fileName = "reports/" + getTimestamp() + "_" + name + ".csv";
    // Create a CSVWriter instance to write the report
    try (Writer writer = new FileWriter(fileName);
      CSVWriter csvWriter = new CSVWriter(writer)) {
      // Write the headers to the CSV file
      csvWriter.writeNext(headers);
      // Write the data to the CSV file
      for (String[] dataRow : data) {
        csvWriter.writeNext(dataRow);
      }
      //
      // Flush the writer to ensure all data is written
      csvWriter.flush();
    } catch (IOException e) {
      logger.error("Error writing report: {}", e.getMessage());
      throw e;
    } catch (Exception e) {
      logger.error("Error writing report: {}", e.getMessage());
      throw e;
    }
    logger.info("Report generated successfully: {}", name);
  }

  public Report addData(String[] dataRow) {
    // Add logic to add data to the report
    logger.info("Adding data to report: {}", name);
    this.data.add(dataRow);
    return this;
  }

  private static String getTimestamp() {
    String format = "yyyy-MM-dd_HH-mm-ss";
    // Add logic to get the current timestamp
    logger.info("Getting current timestamp");
    return new java.text.SimpleDateFormat(format).format(new java.util.Date());
  }

  /**
   * Creates a directory for storing reports if it doesn't already exist.
   */
  private void createDirectory() {
    // Add logic to create the directory
    File directory = new File("reports");
    if (!directory.exists()) {
      directory.mkdir();
      logger.info("Directory created: {}", directory.getAbsolutePath());
    } else {
      logger.info("Directory already exists: {}", directory.getAbsolutePath());
    }
  }
}
