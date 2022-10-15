package alexeq.parquet.sample;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

public class SampleWriter {

    public static void main(String[] args) {
        for(int i = 0; i < 2; i++) {
            generateParquetFileFor(LocalDateTime.now().plusDays(i));
        }
    }

    private static void generateParquetFileFor(LocalDateTime dateTime) {
        try {
            Schema schema = parseSchema();
            DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMdd");
            Path path = new Path("pqt/data3_" + fmt.format(dateTime) + ".parquet");

            List<GenericData.Record> recordList = generateRecords(schema, dateTime);

            try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(path)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize((long) ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withConf(new Configuration())
                .withValidation(true)
                .withDictionaryEncoding(true)
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .build()) {

                for (GenericData.Record record : recordList) {
                    writer.write(record);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace(System.out);
        }
    }

    private static Schema parseSchema() {
        String schemaJson = "{"
            + "\"name\": \"Quote\","
            + "\"namespace\": \"alexeq.parquet.sample\","
            + "\"type\": \"record\","
            + "\"fields\": ["
            + " {\"name\": \"myString\",  \"type\": [\"string\", \"null\"]}"
            + ", {\"name\": \"myInteger\", \"type\": \"int\"}"
            + ", {\"name\": \"myDateTime\", \"type\": [{\"type\": \"long\", \"logicalType\" : \"timestamp-millis\"}, \"null\"]}"
            + " ]}";

        Schema.Parser parser = new Schema.Parser().setValidate(true);
        return parser.parse(schemaJson);
    }

    private static List<GenericData.Record> generateRecords(Schema schema, LocalDateTime dateTime) {

        List<GenericData.Record> recordList = new ArrayList<>();

        long secondsOfDay = 24 * 60 * 60;

        for(int i = 1; i <= secondsOfDay; i++) {
            LocalDateTime dateTimeTmp = dateTime.plus(i-1, ChronoUnit.SECONDS);

            GenericData.Record record = new GenericData.Record(schema);
            record.put("myInteger", i);
            record.put("myString", i + " hi world of parquet!");
            record.put("myDateTime", dateTimeTmp.toInstant(ZoneOffset.UTC).toEpochMilli());

            recordList.add(record);
        }

        return recordList;
    }}
