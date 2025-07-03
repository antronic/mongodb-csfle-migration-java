package app.migrator.csfle.misc;

import java.util.List;

import lombok.Getter;
import lombok.Setter;

public class BoxPrinter {
    @Getter
    private String content = "";
    @Setter
    private int width = 0;

    public static void print(String content) {
        String _content = BoxPrinter.generateContent(content);
        System.out.println(_content);
    }

    public static String generateContent(String content) {
        int padding = 2;
        int width = content.length() + (padding * 2);

        BoxPrinter boxPrinter = new BoxPrinter();
        boxPrinter.setWidth(width);

        boxPrinter.addNewLine();
        boxPrinter.addLine(width);
        boxPrinter.addNewLine();
        boxPrinter.addEmptyLine(width);
        boxPrinter.addNewLine();
        boxPrinter.addContentLine(content, padding);
        boxPrinter.addNewLine();
        boxPrinter.addEmptyLine(width);
        boxPrinter.addNewLine();
        boxPrinter.addLine(width);

        return boxPrinter.getContent();
    }

    public static String generateContent(List<String> contents) {
        int padding = 2;
        int width = contents.stream()
            .mapToInt(String::length).max()
            .orElse(0) + (padding * 2);

        BoxPrinter boxPrinter = new BoxPrinter();
        boxPrinter.addNewLine();
        boxPrinter.addLine(width);
        boxPrinter.addNewLine();
        boxPrinter.addEmptyLine(width);
        boxPrinter.addNewLine();

        for (String content : contents) {
            // Calculate padding to center the content
            int totalPadding = width - content.length();
            int leftPadding = totalPadding / 2;
            int rightPadding = totalPadding - leftPadding; // This handles odd/even differences

            boxPrinter.addContentLineWithDifferentPadding(content, leftPadding, rightPadding);
            boxPrinter.addNewLine();
        }

        boxPrinter.addEmptyLine(width);
        boxPrinter.addNewLine();
        boxPrinter.addLine(width);
        return boxPrinter.getContent();
    }

    private void addEmptyLine(int width) {
        // System.out.print("|");
        // for (int i = 0; i < width; i++) System.out.print(" ");
        // System.out.println("|");
        this.content += "|";
        for (int i = 0; i < width; i++) this.content += " ";
        this.content += "|";
    }

    private void addLine(int width) {
        // System.out.print("+");
        this.content += "+";
        for (int i = 0; i < width; i++) {
            // System.out.print("-");
            this.content += "-";
        }
        this.content += "+";
        // System.out.println("+");
    }

    private void addContentLine(String content, int padding) {
        this.content += "|";

        // If the content length plus padding is odd, add one extra space to the right
        int leftPadding = padding;
        int rightPadding = padding;

        // If there's an uneven amount of padding needed, add the extra space to the right
        if ((width - content.length()) % 2 != 0) {
            rightPadding++;
        }

        for (int i = 0; i < leftPadding; i++) this.content += " ";
        this.content += content;
        for (int i = 0; i < rightPadding; i++) this.content += " ";
        this.content += "|";
    }

    // New method that allows different left and right padding
    private void addContentLineWithDifferentPadding(String content, int leftPadding, int rightPadding) {
        this.content += "|";
        for (int i = 0; i < leftPadding; i++) this.content += " ";
        this.content += content;
        for (int i = 0; i < rightPadding; i++) this.content += " ";
        this.content += "|";
    }

    private void addNewLine() {
        this.content += "\n";
    }
}