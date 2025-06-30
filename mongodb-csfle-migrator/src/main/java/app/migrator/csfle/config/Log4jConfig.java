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

        // Update the rolling pattern with the new date format
        String filePattern = logDir + "/" + appName + "-%d{yyyy-MM-dd_HH-mm-ss}-%i.log.gz";

        // 2. Define the pattern layout with the new date format
        LayoutComponentBuilder layout = builder.newLayout("PatternLayout")
            .addAttribute("pattern", "%d{yyyy-MM-dd_HH-mm-ss} [%t] %-5level %logger{36} - %msg%n");

        // 3. Define the console appender
        AppenderComponentBuilder console = builder.newAppender("Console", "CONSOLE")
            .add(layout);
        builder.add(console);

        // 4. Define the rolling file appender with complete policies
        AppenderComponentBuilder rollingFile = builder.newAppender("FileLogger", "RollingFile")
            .addAttribute("fileName", logFile)
            .addAttribute("filePattern", filePattern)
            .add(layout)
            .addComponent(builder.newComponent("Policies")
                .addComponent(builder.newComponent("SizeBasedTriggeringPolicy")
                    .addAttribute("size", "100MB"))
                .addComponent(builder.newComponent("TimeBasedTriggeringPolicy")
                    .addAttribute("interval", "1")
                    .addAttribute("modulate", "true"))
                .addComponent(builder.newComponent("OnStartupTriggeringPolicy")))
            .addComponent(builder.newComponent("DefaultRolloverStrategy")
                .addAttribute("max", "20")
                .addAttribute("fileIndex", "min"));
        builder.add(rollingFile);

        // 5. Add loggers
        builder.add(builder.newLogger("org.mongodb.driver", Level.OFF));

        builder.add(builder.newRootLogger(Level.INFO)
            .add(builder.newAppenderRef("Console"))
            .add(builder.newAppenderRef("FileLogger")));

        builder.add(
            builder.newLogger("app.migrator.csfle", Level.DEBUG)
            // prevent log propagation to parent loggers
            .addAttribute("additivity", false)
            .add(builder.newAppenderRef("Console"))
            .add(builder.newAppenderRef("FileLogger")));

        // 6. Initialize configuration
        Configurator.initialize(builder.build());
    }
}