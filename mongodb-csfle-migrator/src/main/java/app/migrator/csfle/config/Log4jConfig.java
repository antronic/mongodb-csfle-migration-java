package app.migrator.csfle.config;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.api.LayoutComponentBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

public class Log4jConfig {

    public static void main(String[] args) {
        configureLogging();

        Logger logger = LogManager.getLogger(Log4jConfig.class);
        // logger.info("This is an info message");
        // logger.error("This is an error message");
    }

    private static void configureLogging() {
        ConfigurationBuilder<BuiltConfiguration> builder = ConfigurationBuilderFactory.newConfigurationBuilder();

        builder.setStatusLevel(Level.WARN);
        builder.setConfigurationName("DefaultLogger");

        // 1. Set variables
        String logDir = System.getProperty("logDir", "logs");
        String appName = System.getProperty("appName", "mdb-csfle-migrator");

        // Include date and time in the main log file name with the specified format
        String dateTime = java.time.LocalDateTime.now().format(
            java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss")
        );
        String logFile = logDir + "/" + appName + "-" + dateTime + ".log";
        String errorLogFile = logDir + "/" + appName + "-" + dateTime + "-error.log";

        // Update the rolling pattern with the new date format
        String filePattern = logDir + "/" + appName + "-%d{yyyy-MM-dd_HH-mm-ss}-%i.log.gz";
        String errorFilePattern = logDir + "/" + appName + "-%d{yyyy-MM-dd_HH-mm-ss}-%i-error.log.gz";

        // 2. Define the pattern layout with the new date format
        LayoutComponentBuilder layout = builder.newLayout("PatternLayout")
            .addAttribute("pattern", "%d{yyyy-MM-dd_HH-mm-ss} [%t] %-5level %logger{36} - %msg%n");

        // 3. Define the console appender
        AppenderComponentBuilder console = builder.newAppender("Console", "CONSOLE")
            .add(layout);
        builder.add(console);

        // 4. Define the rolling file appender with complete policies for main log
        AppenderComponentBuilder rollingFile = builder.newAppender("FileLogger", "RollingFile")
            .addAttribute("fileName", logFile)
            .addAttribute("filePattern", filePattern)
            .add(layout)
            .addComponent(builder.newComponent("Policies")
                .addComponent(builder.newComponent("SizeBasedTriggeringPolicy")
                    .addAttribute("size", "100MB"))
                .addComponent(builder.newComponent("OnStartupTriggeringPolicy")))
            .addComponent(builder.newComponent("DefaultRolloverStrategy")
                .addAttribute("max", "30")
                .addAttribute("fileIndex", "min"));
        builder.add(rollingFile);

        // 5. Define error log file with a built-in threshold filter and immediateFlush=true
        AppenderComponentBuilder errorFile = builder.newAppender("ErrorFileLogger", "RollingFile")
            .addAttribute("fileName", errorLogFile)
            .addAttribute("filePattern", errorFilePattern)
            .addAttribute("immediateFlush", "true")
            .addAttribute("createOnDemand", "true") // This creates the file only when needed
            .add(layout)
            // Add filter directly to the appender
            .addComponent(builder.newComponent("ThresholdFilter")
                .addAttribute("level", "ERROR")
                .addAttribute("onMatch", "ACCEPT")
                .addAttribute("onMismatch", "DENY"))
            .addComponent(builder.newComponent("Policies")
                .addComponent(builder.newComponent("SizeBasedTriggeringPolicy")
                    .addAttribute("size", "50MB"))
                .addComponent(builder.newComponent("OnStartupTriggeringPolicy")))
            .addComponent(builder.newComponent("DefaultRolloverStrategy")
                .addAttribute("max", "30")
                .addAttribute("fileIndex", "min"));
        builder.add(errorFile);

        // 6. Add loggers
        builder.add(builder.newLogger("org.mongodb.driver", Level.OFF));

        builder.add(builder.newRootLogger(Level.INFO)
            .add(builder.newAppenderRef("Console"))
            .add(builder.newAppenderRef("FileLogger"))
            .add(builder.newAppenderRef("ErrorFileLogger")));

        builder.add(
            builder.newLogger("app.migrator.csfle", Level.DEBUG)
            // prevent log propagation to parent loggers
            .addAttribute("additivity", false)
            .add(builder.newAppenderRef("Console"))
            .add(builder.newAppenderRef("FileLogger"))
            .add(builder.newAppenderRef("ErrorFileLogger")));

        // 7. Initialize configuration
        Configurator.initialize(builder.build());
    }
}