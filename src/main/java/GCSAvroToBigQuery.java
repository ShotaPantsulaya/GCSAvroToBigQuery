import com.google.api.gax.paging.Page;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;

public class GCSAvroToBigQuery {

    private static final Logger LOGGER = LoggerFactory.getLogger(GCSAvroToBigQuery.class);
    private static final String FILE_PATTERN = "gs://bd_test/test/test/expedia/*.avro";
    private static final String SCHEMA = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"topLevelRecord\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"id\",\n" +
            "      \"type\": [\n" +
            "        \"long\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"date_time\",\n" +
            "      \"type\": [\n" +
            "        \"string\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"site_name\",\n" +
            "      \"type\": [\n" +
            "        \"int\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"posa_continent\",\n" +
            "      \"type\": [\n" +
            "        \"int\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"user_location_country\",\n" +
            "      \"type\": [\n" +
            "        \"int\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"user_location_region\",\n" +
            "      \"type\": [\n" +
            "        \"int\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"user_location_city\",\n" +
            "      \"type\": [\n" +
            "        \"int\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"orig_destination_distance\",\n" +
            "      \"type\": [\n" +
            "        \"double\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"user_id\",\n" +
            "      \"type\": [\n" +
            "        \"int\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"is_mobile\",\n" +
            "      \"type\": [\n" +
            "        \"int\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"is_package\",\n" +
            "      \"type\": [\n" +
            "        \"int\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"channel\",\n" +
            "      \"type\": [\n" +
            "        \"int\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"srch_ci\",\n" +
            "      \"type\": [\n" +
            "        \"string\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"srch_co\",\n" +
            "      \"type\": [\n" +
            "        \"string\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"srch_adults_cnt\",\n" +
            "      \"type\": [\n" +
            "        \"int\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"srch_children_cnt\",\n" +
            "      \"type\": [\n" +
            "        \"int\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"srch_rm_cnt\",\n" +
            "      \"type\": [\n" +
            "        \"int\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"srch_destination_id\",\n" +
            "      \"type\": [\n" +
            "        \"int\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"srch_destination_type_id\",\n" +
            "      \"type\": [\n" +
            "        \"int\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"hotel_id\",\n" +
            "      \"type\": [\n" +
            "        \"long\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    public interface AvroToBQOptions extends PipelineOptions {

        @Description("File pattern for avro ingestion")
        @Validation.Required
        ValueProvider<String> getInputPattern();

        void setInputPattern(ValueProvider<String> inputPattern);

        @Description("BigQuery schema for writing")
        @Validation.Required
        ValueProvider<String> getBQSchema();

        void setBQSchema(ValueProvider<String> schema);

    }

    public static void main(String[] args) {
        PipelineOptionsFactory.register(AvroToBQOptions.class);
        AvroToBQOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(AvroToBQOptions.class);
        options.setRunner(DirectRunner.class);
        Schema avroSchema = new Schema.Parser().parse(SCHEMA); // GET SCHEMA FROM OPTIONS?
        options.setBQSchema(ValueProvider.StaticValueProvider.of(SCHEMA));
        options.setTempLocation("gs://bd_test/tmp");
        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply(AvroIO
                        .readGenericRecords(avroSchema)
                        .from(options.getInputPattern()))
                .apply(ParDo.of(
                        new DoFn<GenericRecord, TableRow>() {
                            @ProcessElement
                            public void process(ProcessContext context) {
                                GenericRecord element = context.element();
                                AvroToBQOptions bqOptions = context.getPipelineOptions().as(AvroToBQOptions.class);
                                String stringSchema = bqOptions.getBQSchema().get();
                                TableSchema tableSchema = BigQueryUtils.toTableSchema(AvroUtils.toBeamSchema(new Schema.Parser().parse(stringSchema)));
                                context.output(BigQueryUtils.convertGenericRecordToTableRow(element, tableSchema));
                            }
                        }
                ))
                .apply(
                        "WriteToBQ",
                        BigQueryIO
                                .writeTableRows()
                                .to(new TableReference()
                                        .setProjectId("big-data-201")
                                        .setDatasetId("test")
                                        .setTableId("avro_table_parsed"))
                                .withSchema(
                                        BigQueryUtils.toTableSchema(AvroUtils.toBeamSchema(new Schema.Parser().parse(options.getBQSchema().get())))
                                )
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                );
        pipeline.run(options).waitUntilFinish();
    }

    /*private static Blob getAvroFile(String bucketString, String prefix) throws FileNotFoundException {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Page<Blob> bucket = storage.list(bucketString, Storage.BlobListOption.prefix(prefix));
        Iterable<Blob> values = bucket.getValues();
        while (values != null) {
            for (Blob value : values) {
                if (value.getName().matches("^.+\\.avro$")) {
                    System.out.println(value.getName());
                    return value;
                }
            }
            values = bucket.hasNextPage() ? bucket.getNextPage().getValues() : null;
        }
        throw new FileNotFoundException("Cannot find avro file in the selected path"); //Change to API exception
    }

    private static Schema getAvroSchemaFromBlob(Blob blob) throws IOException {
        ReadChannel reader = blob.reader();
        InputStream inputStream = Channels.newInputStream(reader);

        return new Schema.Parser().parse(inputStream);
    }*/

}
