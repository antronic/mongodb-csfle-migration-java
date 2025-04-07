package app.migrator.csfle;

import app.migrator.csfle.config.Configuration;
import app.migrator.csfle.service.MongoCSFLE;
import lombok.Getter;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;

@Command(name = "mongodb-migrator-csfle", mixinStandardHelpOptions = true, version = "1.0.1e-beta",
    subcommands = {MigrateCommand.class, GenerateDekIdCommand.class},
    description = "CLI app with required command and optional config files")
public class CSFLEMigratorApp implements Runnable {

    @Getter
    @Option(names = {"-c", "--config"}, description = "Path to config.json")
    private String configPath = "config.json";

    @Getter
    @Option(names = {"-s", "--schema"}, description = "Path to schema.json")
    private String schemaPath = "schema.json";

    @Getter
    @Option(names = {"-t", "--migrate-config"}, description = "Path to migrate-config.json")
    private String migrationConfig = "migration-config.json";

    @Spec CommandSpec spec;
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

@Command(name = "migrate", description = "Migrate data from one MongoDB instance to another")
class MigrateCommand implements Runnable {

    @ParentCommand
    private CSFLEMigratorApp parent;

    @Override
    public void run() {
        // Configuration files
        String configPath = parent.getConfigPath();
        String schemaPath = parent.getSchemaPath();
        String migrationConfig = parent.getMigrationConfig();

        Configuration configuration = Configuration.load(configPath);
        configuration
            .loadMigrateTarget(migrationConfig)
            .loadSchema(schemaPath);

        System.out.println("Configuration loaded:");
        System.out.println("Source MongoDB URI: " + configuration.getSourceMongoDBUri());
        System.out.println("Target MongoDB URI: " + configuration.getTargetMongoDBUri());
        System.out.println(configuration.getMigrationConfig());

        MigrationDriver driver = new MigrationDriver(configuration);
        driver.setup();
        driver.startMigration();
    }
}

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

        MongoCSFLE mongoCSFLE = new MongoCSFLE(configuration.getTargetMongoDBUri(), configuration);
        mongoCSFLE.setup();
        mongoCSFLE.preConfigure();
        String dekId = mongoCSFLE.generateDataKey();
        System.out.println("Generated DEK ID: " + dekId);
    }
}