package ch3.proto;

import au.com.bytecode.opencsv.CSVParser;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * Created by hua on 30/06/16.
 */
public class StockUtils {

    private static CSVParser parser = new CSVParser();

    public static List<StockProtos.Stock> fromCsvFile(File file) throws IOException {
        return fromCsvStream(FileUtils.openInputStream(file));
    }

    public static List<StockProtos.Stock> fromCsvStream(InputStream is) throws IOException {
        List<StockProtos.Stock> stocks = Lists.newArrayList();
        for(String line: IOUtils.readLines(is)) {
            stocks.add(fromCsv(line));
        }
        is.close();
        return stocks;
    }

    public static StockProtos.Stock fromCsv(String line) throws IOException {
        String parts[] = parser.parseLine(line);
        return StockProtos.Stock.newBuilder()
                .setSymbol(parts[0])
                .setDate(parts[1])
                .setOpen(Double.valueOf(parts[2]))
                .setHigh(Double.valueOf(parts[3]))
                .setLow(Double.valueOf(parts[4]))
                .setClose(Double.valueOf(parts[5]))
                .setVolume(Integer.valueOf(parts[6]))
                .setAdjClose(Double.valueOf(parts[7])).build();
    }
}
