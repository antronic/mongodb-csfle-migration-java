package app.migrator.csfle.misc;

public class BoxPrinter {
    public static void print(String content) {
        int padding = 2;
        int width = content.length() + (padding * 2);

        printLine(width);
        printEmptyLine(width);
        printContentLine(content, padding);
        printEmptyLine(width);
        printLine(width);
    }

    static void printEmptyLine(int width) {
        System.out.print("|");
        for (int i = 0; i < width; i++) System.out.print(" ");
        System.out.println("|");
    }

    static void printLine(int width) {
        System.out.print("+");
        for (int i = 0; i < width; i++) System.out.print("-");
        System.out.println("+");
    }

    static void printContentLine(String content, int padding) {
        System.out.print("|");
        for (int i = 0; i < padding; i++) System.out.print(" ");
        System.out.print(content);
        for (int i = 0; i < padding; i++) System.out.print(" ");
        System.out.println("|");
    }
}