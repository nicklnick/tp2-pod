package ar.edu.itba.pod.hazelcast.file;

import com.opencsv.CSVWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class CsvFileWriter {

    private static final char SEPARATOR = ';';

    private CsvFileWriter() {
        throw new AssertionError("This class should not be instantiated");
    }

    public static void writeRows(String filePath, List<String[]> rows) {
        try (CSVWriter writer = new CSVWriter(new FileWriter(filePath), SEPARATOR,
                CSVWriter.NO_QUOTE_CHARACTER,
                CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                CSVWriter.DEFAULT_LINE_END)) {
            for (String[] row : rows) {
                writer.writeNext(row);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeLog(String filePath, List<String> logMessages) {
        try (FileWriter writer = new FileWriter(filePath)) {
            for (String logMessage : logMessages) {
                writer.write(logMessage + System.lineSeparator());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
