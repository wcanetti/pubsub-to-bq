package org.apache.beam;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Properties;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.nio.charset.StandardCharsets;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.joda.time.Duration;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.GoogleCredentials;

class ConvertJSONtoTableRow extends DoFn<String, TableRow> {

    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void processElement(ProcessContext c) {
        // Parse the JSON into a {@link TableRow} object.
        String json = c.element();
        TableRow row_internal;
        try (InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            row_internal = TableRowJsonCoder.of().decode(inputStream, Coder.Context.OUTER);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize json to table row: " + json, e);
        }
        c.output(row_internal);
    }

}

public class cdc {

    public static class GetPropertyValues {

        String result = "";
        InputStream inputStream;

        public String getPropValues(String property) throws IOException {

            Properties prop = new Properties();
            String propFileName = "configuration.properties";

            inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }

            String result = prop.getProperty(property);
            inputStream.close();

            return result;
        }
    }

    public interface PubSubToBQOptions extends DataflowPipelineOptions {

        void setProject(String string);

        @Description("GCS temp bucket for BQ Loads")
        @Required
        ValueProvider<String> getTempGCSBQBucket();
        void setTempGCSBQBucket(ValueProvider<String> value);

        @Description("GCS temp bucket for Dataflow Processing")
        @Required
        String getTempGCSDataflowBucket();
        void setTempGCSDataflowBucket(String value);

    }
    
    public static void main(String args[]) throws IOException {

        GetPropertyValues objProperty = new GetPropertyValues();
        String project = objProperty.getPropValues("project");

        Storage storage = StorageOptions.newBuilder()
            .setProjectId(project)
            // .setCredentials(GoogleCredentials.fromStream(new FileInputStream(serviceAccountJSON)))
            .build()
            .getService();

        PubSubToBQOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToBQOptions.class);
        options.setProject(project);
        options.setStreaming(true);
        options.setEnableStreamingEngine(true);
        
        Pipeline pipeline = Pipeline.create(options);

        String database_instance = objProperty.getPropValues("database_instance");
        String source_db_schema = objProperty.getPropValues("source_db_schema");
        String[] tables = objProperty.getPropValues("tables").split(",");
        String topics_prefix = objProperty.getPropValues("topics_prefix");
        LinkedHashMap<String, PCollection<String>> hash_topic_pcol = new LinkedHashMap<String, PCollection<String>>();
        LinkedHashMap<String, String> hash_schema = new LinkedHashMap<String, String>();
        
        String topic = "";
        for (String table : tables) {
            topic = database_instance + "." + source_db_schema + "." + table;
            Blob blob = storage.get(objProperty.getPropValues("bucket_url"), objProperty.getPropValues("bucket_schema_root_path") + "schema_" + table + ".json");
            String fileContent = new String(blob.getContent());
            hash_schema.put(topic, fileContent);
            hash_topic_pcol.put(topic, pipeline.apply("Read PubSub Messages from " + topic, 
                                                      PubsubIO.readStrings().fromTopic(topics_prefix + topic)));       
        }
        
        String bq_dataset = objProperty.getPropValues("bq_dataset");
        String bq_table = "";
        String bq_schema = "";
        for (String topic_hash_key : hash_topic_pcol.keySet()) {
            int first = topic_hash_key.indexOf(".");
            int second = topic_hash_key.indexOf(".", first + 1);
            bq_table = topic_hash_key.substring(second + 1) + "_delta";
            bq_schema = hash_schema.get(topic_hash_key);
            hash_topic_pcol.get(topic_hash_key).apply("Convert messages to TableRows " + topic_hash_key, ParDo.of(new ConvertJSONtoTableRow()))
                                               .apply("Write to BigQuery table " + bq_table , BigQueryIO.writeTableRows()
                                                    .to(String.format("%s:%s.%s", project, bq_dataset, bq_table))
                                                    .withJsonSchema(bq_schema)
                                                    .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                                                    .withTriggeringFrequency(Duration.standardMinutes(2))
                                                    .withCustomGcsTempLocation(options.getTempGCSBQBucket())
                                                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                                    .withoutValidation());

        }
        
        pipeline.run().waitUntilFinish();

    }

}
