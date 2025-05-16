package app.migrator.csfle;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.migrator.csfle.config.Configuration;
import app.migrator.csfle.misc.BoxPrinter;
import app.migrator.csfle.service.MongoCSFLE;
import app.migrator.csfle.service.MongoDBService;
import app.migrator.csfle.service.Report;
import lombok.Getter;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

/**
 * MongoDB CSFLE Migration Tool
 *
 * This application facilitates the migration of data between MongoDB instances
 * with support for Client-Side Field Level Encryption (CSFLE).
 */
@Command(name = "mongodb-migrator-csfle", mixinStandardHelpOptions = true, version = "1.0.1e-beta",
    subcommands = {MigrateCommand.class, GenerateDekIdCommand.class, ShowConfigCommand.class, ValidateCommand.class, FeatureTestCommand.class},
    description = "CLI app with required command and optional config files")
public class CSFLEMigratorApp implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(CSFLEMigratorApp.class);

    //----------------------------------------------------------------------
    // Configuration Options
    //----------------------------------------------------------------------

    @Getter
    @Option(names = {"-c", "--config"}, description = "Path to config.json")
    private String configPath = "config.json";

    @Getter
    @Option(names = {"-s", "--schema"}, description = "Path to schema.json")
    private String schemaPath = "schema.json";

    @Spec CommandSpec spec;

    //----------------------------------------------------------------------
    // Main Logic
    //----------------------------------------------------------------------

    public void run() {
        // your app logic here
        spec.commandLine()
            .usage(System.err);
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new CSFLEMigratorApp()).execute(args);
        System.exit(exitCode);
    }
}

//==============================================================================
// MIGRATE COMMAND
//==============================================================================

/**
 * Handles the migration of data between MongoDB instances.
 * Supports configuration of source and target databases, along with
 * encryption settings.
 */
@Command(name = "migrate", description = "Migrate data from one MongoDB instance to another")
class MigrateCommand implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(MigrateCommand.class);

    @Option(names = {"-t", "--migrate-config"}, description = "Path to migrate-config.json")
    private String migrationConfig = "migration-config.json";

    @ParentCommand
    private CSFLEMigratorApp parent;

    @Override
    public void run() {
        // Configuration files
        String configPath = parent.getConfigPath();
        String schemaPath = parent.getSchemaPath();

        Configuration configuration = Configuration.load(configPath);
        configuration
            .loadMigrateTarget(migrationConfig)
            .loadSchema(schemaPath);

        logger.info("Configuration loaded:");
        logger.debug(configuration.toString());

        MigrationDriver driver = new MigrationDriver(configuration);
        driver.setup();
        driver.startMigration();
    }
}

//==============================================================================
// GENERATE DEK ID COMMAND
//==============================================================================

/**
 * Generates a new Data Encryption Key (DEK) ID for use with CSFLE.
 * This key ID can be used in the schema configuration for encrypted fields.
 */
@Command(name = "generate-dekid", description = "Generate DEK ID for a given key")
class GenerateDekIdCommand implements Runnable {
    @ParentCommand
    private CSFLEMigratorApp parent;

    @Option(names = {"-n", "--name"}, description = "Specify the altKeyName for key")
    private String keyAltName = "default";

    @Override
    public void run() {
        // Configuration files
        String configPath = parent.getConfigPath();
        String schemaPath = parent.getSchemaPath();
        Configuration config = Configuration.load(configPath)
            .loadSchema(schemaPath);

        String dekId = null;

        try (MongoDBService mongoService = new MongoDBService(config.getTargetMongoDB())) {
            mongoService.setup();
            MongoCSFLE mongoCSFLE = new MongoCSFLE(config.getTargetMongoDB().getUri(), config);
            mongoCSFLE
                    .setMongoClientSettingsBuilder(mongoService.getMongoClientSettingsBuilder())
                    .setup()
                    .setMongoClient(mongoService.getClient())
                    .preConfigure();
            //
            // Generate DEK ID
            //
            dekId = mongoCSFLE.generateDataKey(keyAltName);
        } finally {
            System.out.println();
            BoxPrinter.print("Generated DEK ID: " + dekId);
            System.out.println();
        }
    }
}

//==============================================================================
// SHOW CONFIG COMMAND
//==============================================================================

/**
 * Displays the current configuration settings.
 * Useful for verification before running migration operations.
 */
@Command(name="show-config", description = "Show the current configuration")
class ShowConfigCommand implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(ShowConfigCommand.class);

    @Option(names = {"-t", "--migrate-config"}, description = "Path to migrate-config.json")
    private String migrationConfig = "migration-config.json";

    @ParentCommand
    private CSFLEMigratorApp parent;

    @Override
    public void run() {
        // Configuration files
        String configPath = parent.getConfigPath();
        String schemaPath = parent.getSchemaPath();
        Configuration configuration = Configuration.load(configPath)
            .loadMigrateTarget(migrationConfig)
            .loadSchema(schemaPath);

        logger.info("Configuration loaded:");
        logger.debug(configuration.toString());
    }
}

//==============================================================================
// VALIDATION COMMANDS
//==============================================================================

/**
 * Parent command for validation operations.
 * Groups related validation functionality under a single namespace.
 */
@Command(
    name="validate",
    description = "Validate the configuration",
    subcommands= {
        ValidateCommand.ValidateByCountCommand.class,
        ValidateCommand.ValidateByDocCompareCommand.class
    }
)
class ValidateCommand implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(ValidateCommand.class);

    @Option(names = {"-t", "--validate-config"}, description = "Path to validate-config.json")
    private String validationConfig = "validation-config.json";

    @ParentCommand
    private CSFLEMigratorApp parent;

    Configuration configuration;

    public void setup() {
        // Configuration files
        String configPath = parent.getConfigPath();
        String schemaPath = parent.getSchemaPath();
        // Load configuration
        this.configuration = Configuration.load(configPath)
            .loadValidationTarget(validationConfig)
            .loadSchema(schemaPath);

        logger.info("Configuration loaded:");
        logger.debug(configuration.toString());
    }

    @Override
    public void run() {

    }

    //----------------------------------------------------------------------
    // Validation Subcommands
    //----------------------------------------------------------------------

    /**
     * Validates configuration by counting documents in source collection.
     * Helps ensure data source is properly configured and accessible.
     */
    @Command(name="count", description = "Count the number of documents in the source collection")
    static class ValidateByCountCommand implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(ValidateByCountCommand.class);

        @ParentCommand
        private ValidateCommand parent;

        @Override
        public void run() {
            parent.setup();
            logger.info("Counting documents in source collection...");

            ValidationDriver driver = new ValidationDriver(parent.configuration, ValidationDriver.ValidationStrategy.COUNT);
            driver.setup()
                .start();
            // driver.testConcurrent();
        }
    }

    /**
     *
     */
    @Command(name="doc-compare", description = "Count the number of documents in the source collection")
    static class ValidateByDocCompareCommand implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(ValidateByDocCompareCommand.class);

        @ParentCommand
        private ValidateCommand parent;

        @Override
        public void run() {
            parent.setup();
            logger.info("Counting documents in source collection...");

            ValidationDriver driver = new ValidationDriver(parent.configuration, ValidationDriver.ValidationStrategy.DOC_COMPARE);
            driver.setup()
                .start();
            // driver.testConcurrent();
        }
    }
}

//==============================================================================
// Feature test COMMANDS
//==============================================================================
@Command(
    name="feature-test",
    description = "Feature test commands"
)
class FeatureTestCommand implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(FeatureTestCommand.class);

    @ParentCommand
    private CSFLEMigratorApp parent;

    @Override
    public void run() {
        Report report = new Report("test");

        try {
            report.generate();

        } catch (IOException e) {
            logger.error("Error generating report: " + e.getMessage());
        }
    }
}