package ch5.hbase;

import ch3.avro.AvroStockUtils;
import ch3.avro.gen.Stock;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

/**
 * Created by hua on 26/07/16.
 */
public class HBaseWriter extends Configured implements Tool {

    public static void main(String ... args) throws Exception {
        int exitCode = ToolRunner.run(new HBaseWriter(), args);
        System.exit(exitCode);
    }
    private static final String TABLE_NAME = "stocks_example";
    private static final String CF_DEFAULT = "details";



    public static String STOCKS_TABLE_NAME = "stocks_example";
    public static String STOCK_DETAILS_COLUMN_FAMILY = "details";
    public static byte[] STOCK_DETAILS_COLUMN_FAMILY_AS_BYTES =
            Bytes.toBytes(STOCK_DETAILS_COLUMN_FAMILY);
    public static String STOCK_COLUMN_QUALIFIER = "stockAvro";
    public static byte[] STOCK_COLUMN_QUALIFIER_AS_BYTES =
            Bytes.toBytes(STOCK_COLUMN_QUALIFIER);

    @Override
    public int run(String[] args) throws Exception {

        File inputFile = new File(args[0]);

        HTable htable = new HTable(getConf(), STOCKS_TABLE_NAME);
        htable.setWriteBufferSize(1024 * 1024 * 12);

        SpecificDatumWriter<Stock> writer =
                new SpecificDatumWriter<Stock>();
        writer.setSchema(Stock.SCHEMA$);


        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        BinaryEncoder encoder =
                EncoderFactory.get().directBinaryEncoder(bao, null);

        for (Stock stock: AvroStockUtils.fromCsvFile(inputFile)) {
            writer.write(stock, encoder);
            encoder.flush();

            byte[] rowkey = Bytes.add(
                    Bytes.toBytes(stock.getSymbol().toString()),
                    Bytes.toBytes(stock.getDate().toString()));

            byte[] stockAsAvroBytes = bao.toByteArray();

            Put put = new Put(rowkey);

            Cell cell = CellUtil.createCell(STOCK_DETAILS_COLUMN_FAMILY_AS_BYTES, STOCK_COLUMN_QUALIFIER_AS_BYTES, stockAsAvroBytes);

            put.add(cell);

            htable.put(put);

            bao.reset();
        }

        htable.flushCommits();
        htable.close();
        System.out.println("done");
        return 0;
    }

    public static void createTableAndColumn() throws Exception {

        Configuration config = HBaseConfiguration.create();

        //Add any necessary configuration files (hbase-site.xml, core-site.xml)
        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        createSchemaTables(config);

    }

    public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }

    public static void createSchemaTables(Configuration config) throws IOException {
        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();

        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        table.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Compression.Algorithm.SNAPPY));

        System.out.print("Creating table. ");
        createOrOverwrite(admin, table);
        System.out.println(" Done.");

    }

    public static void modifySchema (Configuration config) throws IOException {
        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();

        TableName tableName = TableName.valueOf(TABLE_NAME);
        if (admin.tableExists(tableName)) {
            System.out.println("Table does not exist.");
            System.exit(-1);
        }

        HTableDescriptor table = new HTableDescriptor(tableName);

        // Update existing table
        HColumnDescriptor newColumn = new HColumnDescriptor("NEWCF");
        newColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
        newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
        admin.addColumn(tableName, newColumn);

        // Update existing column family
        HColumnDescriptor existingColumn = new HColumnDescriptor(CF_DEFAULT);
        existingColumn.setCompactionCompressionType(Compression.Algorithm.GZ);
        existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
        table.modifyFamily(existingColumn);
        admin.modifyTable(tableName, table);

        // Disable an existing table
        admin.disableTable(tableName);

        // Delete an existing column family
        admin.deleteColumn(tableName, CF_DEFAULT.getBytes("UTF-8"));

        // Delete a table (Need to be disabled first)
        admin.deleteTable(tableName);

    }
}
