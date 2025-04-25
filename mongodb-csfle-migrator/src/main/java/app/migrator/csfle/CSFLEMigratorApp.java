package app.migrator.csfle;

import app.migrator.csfle.config.Configuration;
import app.migrator.csfle.service.MongoCSFLE;
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
    subcommands = {MigrateCommand.class, GenerateDekIdCommand.class, ShowConfigCommand.class, ValidateCommand.class},
    description = "CLI app with required command and optional config files")
public class CSFLEMigratorApp implements Runnable {

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

        System.out.println("Configuration loaded:");
        System.out.println("Source MongoDB URI: " + configuration.getSourceMongoDB().getUri());
        System.out.println("Target MongoDB URI: " + configuration.getTargetMongoDB().getUri());
        System.out.println(configuration.getMigrationConfig());

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

    @Override
    public void run() {
        // Configuration files
        String configPath = parent.getConfigPath();
        String schemaPath = parent.getSchemaPath();
        Configuration configuration = Configuration.load(configPath)
            .loadSchema(schemaPath);

        MongoCSFLE mongoCSFLE = new MongoCSFLE(configuration.getTargetMongoDB().getUri(), configuration);
        mongoCSFLE.setup();
        mongoCSFLE.preConfigure();
        String dekId = mongoCSFLE.generateDataKey();
        System.out.println("Generated DEK ID: " + dekId);
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

    @ParentCommand
    private CSFLEMigratorApp parent;

    @Override
    public void run() {
        // Configuration files
        String configPath = parent.getConfigPath();
        String schemaPath = parent.getSchemaPath();
        Configuration configuration = Configuration.load(configPath)
            .loadSchema(schemaPath);

        System.out.println("Configuration loaded:");
        System.out.println("Source MongoDB URI: " + configuration.getSourceMongoDB().getUri());
        System.out.println("Target MongoDB URI: " + configuration.getTargetMongoDB().getUri());
        System.out.println(configuration);
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
        ValidateCommand.ValidateByCountCommand.class
    }
)
class ValidateCommand implements Runnable {

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

        System.out.println("Configuration loaded:");
        System.out.println("Source MongoDB URI: " + configuration.getSourceMongoDB().getUri());
        System.out.println("Target MongoDB URI: " + configuration.getTargetMongoDB().getUri());
        // System.out.println(configuration);
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

        @ParentCommand
        private ValidateCommand parent;

        @Override
        public void run() {
            parent.setup();
            System.out.println("Counting documents in source collection...");

            ValidationDriver driver = new ValidationDriver(parent.configuration);
            driver.setup();
            driver.startCount();
            // driver.testConcurrent();
        }
    }
}