package ar.edu.itba.pod.hazelcast.file;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CsvFileReader {
    private static final char SEPARATOR = ';';
    private static final int BATCH_SIZE = 2048;

    private CsvFileReader() {
        throw new AssertionError("This class should not be instantiated");
    }

    public static void readRows(String filePath, CsvLineListener listener) {
        try {
            final FileReader fileReader = new FileReader(filePath);
            final CSVReader csvReader = new CSVReaderBuilder(fileReader)
                    .withCSVParser(new CSVParserBuilder()
                            .withSeparator(SEPARATOR)
                            .build()
                    )
                    .withSkipLines(1)
                    .build();

            String[] nextLine;
            while ((nextLine = csvReader.readNext()) != null) {
                listener.onCsvParsedLine(nextLine);
            }

            csvReader.close();
            fileReader.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }
    }

    public static void batchReadRows(String filePath, BatchListener listener) {
        try (
                final FileReader fileReader = new FileReader(filePath);
                final CSVReader csvReader = new CSVReaderBuilder(fileReader)
                        .withCSVParser(new CSVParserBuilder()
                                .withSeparator(SEPARATOR)
                                .build()
                        )
                        .withSkipLines(1)
                        .build()
        ) {
            final List<String[]> lines = new ArrayList<>();
            String[] nextLine;
            while ((nextLine = csvReader.readNext()) != null) {
                lines.add(nextLine);
                if(lines.size() >= BATCH_SIZE) {
                    listener.onBatchParsedLine(new ArrayList<>(lines));
                    lines.clear();
                }
            }
            if(!lines.isEmpty())
                listener.onBatchParsedLine(new ArrayList<>(lines));

        } catch (IOException e) {
            e.printStackTrace();
        } catch (CsvValidationException e) {
            throw new RuntimeException(e);
        }
    }
}
