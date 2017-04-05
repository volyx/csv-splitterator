package io.volyx.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class CsvSpliterator extends FixedBatchSpliteratorBase<CSVRecord> {
    private final Iterator<CSVRecord> it;

    CsvSpliterator(CSVParser cr, int batchSize) {
        super(IMMUTABLE | ORDERED | NONNULL, batchSize);
        if (cr == null) throw new NullPointerException("CSVReader is null");
        this.it = cr.iterator();
    }

    public CsvSpliterator(CSVParser cr) {
        this(cr, 128);
    }

    public static Stream<CSVRecord> csvStream(InputStream in, CSVFormat format) {
        final CSVParser cr;
        try {
            cr = new CSVParser(new InputStreamReader(in), format);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return StreamSupport.stream(new CsvSpliterator(cr), false).onClose(() -> {
            try {
                cr.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    public static void main(String[] args) {
        try (FileInputStream fis = new FileInputStream("C:\\Users\\dima.v\\workspace\\small-data-service\\data\\adjust-1491227450860-1491313851683.csv")) {
            long start = System.currentTimeMillis();
            csvStream(fis, CSVFormat.EXCEL).parallel().map(record -> "name: " + record.get(1)).forEach(System.out::println);
            System.out.println((System.currentTimeMillis() - start) + " ms.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean tryAdvance(Consumer<? super CSVRecord> action) {
        if (action == null) throw new NullPointerException();

        if (!it.hasNext()) return false;
        action.accept(it.next());

        return true;
    }

    @Override
    public void forEachRemaining(Consumer<? super CSVRecord> action) {
        if (action == null) throw new NullPointerException();
        while (it.hasNext()) {
            action.accept(it.next());
        }
    }
}
