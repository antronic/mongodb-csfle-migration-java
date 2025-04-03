package me.jirachai.mongodb.migrator.csfle;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        //
        // If args is empty, show usage
        if (args.length == 0) {
            System.out.println("Usage: java -jar mongodb-csfle-migrator.jar <config.json path> <command> [options]");
            System.out.println("Commands:");
            System.out.println("  migrate   Migrate data from unencrypted to encrypted");
            System.out.println("  decrypt   Decrypt data from encrypted to unencrypted");
            System.out.println("Options:");
            System.out.println("  --help    Show this help message");
            return;
        }

        String configPath = args[0];
    }
}
