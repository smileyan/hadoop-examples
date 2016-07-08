REGISTER /opt/pig/contrib/piggybank/java/piggybank.jar
REGISTER /opt/hadoop/share/hadoop/common/lib/avro-1.8.1.jar
REGISTER /home/hua/tmp/my-app/target/lib/snappy-java-1.1.1.3.jar
REGISTER /opt/hadoop/share/hadoop/httpfs/tomcat/webapps/webhdfs/WEB-INF/lib/json-simple-1.1.jar
REGISTER /opt/hadoop/share/hadoop/common/lib/jackson-*.jar

REGISTER /home/hua/tmp/my-app/target/lib/*.jar


stocks = LOAD '/var/hadoop/stock_pig/' USING
  org.apache.pig.piggybank.storage.avro.AvroStorage();


stocks = LOAD '/var/hadoop/stock_pig/' USING
  parquet.pig.ParquetLoader();


DESCRIBE stocks;

by_symbol = GROUP stocks BY symbol;

symbol_count = foreach by_symbol generate group, COUNT($1);

dump symbol_count;

SET mapred.compress.map.output true;
SET mapred.output.compress true;
SET mapred.output.compression.codec
  org.apache.hadoop.io.compress.SnappyCodec

SET avro.output.codec snappy;
google_stocks = FILTER stocks BY symbol == 'GOOG';
STORE google_stocks INTO '/var/hadoop/stock_pig_output/'
USING org.apache.pig.piggybank.storage.avro.AvroStorage(
  'no_schema_check',
  'data', 'stock_pig/');
