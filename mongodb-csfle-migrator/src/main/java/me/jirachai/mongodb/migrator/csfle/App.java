package me.jirachai.mongodb.migrator.csfle;

import lombok.Getter;
import me.jirachai.mongodb.migrator.csfle.config.Configuration;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;

@Command(name = "mongodb-migrator-csfle", mixinStandardHelpOptions = true, version = "1.0",
    subcommands = {MigrateCommand.class},
    description = "CLI app with required command and optional config files")
public class App implements Runnable {

    @Getter
    @Option(names = {"-c", "--config"}, description = "Path to config.json")
    private String configPath = "config.json";

    @Option(names = {"-s", "--schema"}, description = "Path to schema.json")
    private String schemaPath = "schema.json";

    @Spec CommandSpec spec;
    public void run() {
        // System.out.printf("Command: %s%n", command);
        // System.out.printf("Config: %s%n", configPath != null ? configPath : "not provided");
        // System.out.printf("Schema: %s%n", schemaPath != null ? schemaPath : "not provided");

        // your app logic here
        spec.commandLine()
            .usage(System.err);
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new App()).execute(args);
        System.exit(exitCode);
    }
}

@Command(name = "migrate", description = "Migrate data from one MongoDB instance to another")
class MigrateCommand implements Runnable {

    @Option(names = {"-f", "--from"}, description = "Source MongoDB connection string")
    private String from;

    @Option(names = {"-t", "--to"}, description = "Target MongoDB connection string")
    private String to;

    @ParentCommand
    private App parent;

    @Override
    public void run() {
        String configPath = parent.getConfigPath();

        // System.out.printf("Migrating data from %s to %s%n", from, to);
        // your migration logic here

        // MigrationDriver driver = new MigrationDriver(from, to);
        Configuration _config = Configuration.load(configPath);

        System.out.println(_config.toString());
    }
}