import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.LogManager;
import scala.collection.mutable.StringBuilder;

// PARSING THE JSON
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;

// flink table
import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.common.serialization.SerializationSchema;


import java.util.ArrayList;
import java.util.List;

import java.util.Properties;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Tuple2StringKeySchema implements SerializationSchema<Tuple2<String, String>> {
    @Override
    public byte[] serialize(Tuple2<String, String> element) {
        // Convert the Tuple2 to a byte array
        String key = element.f0;

        return (key).getBytes(); // Customize serialization as needed
    }
}

class Tuple2StringValueSchema implements SerializationSchema<Tuple2<String, String>> {
    @Override
    public byte[] serialize(Tuple2<String, String> element) {
        // Convert the Tuple2 to a byte array
        String value = element.f1;
        return (value).getBytes(); // Customize serialization as needed
    }
}

public class ConsumeFlinkData {

    //protected static final Logger LOG = LogManager.getLogger(ConsumeFlinkData.class);

    private static final Logger LOG = LoggerFactory.getLogger(ConsumeFlinkData.class);

    public static void main(String[] args) throws Exception {

        // for template variables

        String keyField;
        List<String> fields = new ArrayList<>();
        List<String> staticFields = new ArrayList<>();
        List<String> staticValues = new ArrayList<>();

        // TOPICS TO PRODUCE MESSAGES INTO

        String sourceTopic = System.getenv("KAFKA_SOURCE_TOPIC");
        String processedTopic = System.getenv("KAFKA_PROCESSED_TOPIC");
        String fallBackTopic = System.getenv("KAFKA_DLQ_TOPIC");

        // PRODUCER PROPS

        String server = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        String api_key = System.getenv("KAFKA_API_KEY");
        String api_secret = System.getenv("KAFKA_API_SECRET");

        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", server);
        producerProps.setProperty("acks", "1");
        producerProps.setProperty("key.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.setProperty("value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");

        producerProps.setProperty("security.protocol", "SASL_SSL");
        producerProps.setProperty("sasl.mechanism", "PLAIN");
        producerProps.setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + api_key
                        + "\" password=\"" + api_secret + "\";");

        // FOR PROCESSING DATA, TAKES RAW DATA AND FILTERS USING THE TEMPLATES

        // // READ AND PARSE MATCHERS.JSON
        GetMatchers matchers = new GetMatchers();

        String rule = args[0];

        matchers.CheckCondition(rule);

        JSONObject ruleJson = new JSONObject(rule);

        String Key = ruleJson.getString("key_template");
        String Value = ruleJson.getString("value_template");

        // PROCESS KEY TEMPLATE
        Pattern keyPattern = Pattern.compile("\\{\\{(.*?)\\}\\}");
        Matcher keyMatcher = keyPattern.matcher(Key);

        if (keyMatcher.find()) {
            String keyPath = keyMatcher.group(1).replace(".", "/");
            keyField = "/" + keyPath;
        } else {
            throw new IllegalArgumentException("Invalid key template format.");
        }

        // PROCESS VALUE TEMPLATE

        Pattern valuePattern = Pattern.compile("\\{\\{(.*?)\\}\\}");
        Matcher valueMatcher = valuePattern.matcher(Value);
        while (valueMatcher.find()) {
            String valuePath = valueMatcher.group(1).replace(".", "/");
            String valueField = "/" + valuePath;
            valueField = valueField.replaceAll("\\s", "");

            fields.add(valueField);
        }

        JSONObject valueTemplateJson = new JSONObject(Value);
        for (String staticField : valueTemplateJson.keySet()) {
            String value = valueTemplateJson.getString(staticField);
            if (!(valueTemplateJson.getString(staticField).matches("\\{\\{.*?\\}\\}"))) {
                staticFields.add(staticField);
                if (!value.matches("\\{\\{.*?\\}\\}")) {

                    staticValues.add(value);
                }
            }
        }

        System.out.println(" KEY : " + Key + "\n");
        System.out.println(" value : " + fields + "\n");
        System.out.println(" static fields : " + staticFields + "\n");
        System.out.println(" staticvalue : " + staticValues + "\n");

        // ------- CREATE STREAM ENVIRONMENT AND TABLE SOURCE -------------

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // ------------- KAFKA SOURCE BUILD ----------------

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setProperties(producerProps)
                .setTopics(sourceTopic)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // ------------- MAKE A KAFKA DATASTREAM ----------------
        DataStream<String> userDataStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "APIlog_Source");

        // FOR REQUESTS CONTAINING MISSING VALUES
        KafkaRecordSerializationSchema<Tuple2<String, String>> missingDataSchema = KafkaRecordSerializationSchema
                .<Tuple2<String, String>>builder()
                .setTopic(fallBackTopic)
                .setKeySerializationSchema(new Tuple2StringKeySchema())
                .setValueSerializationSchema(new Tuple2StringValueSchema())
                .build();

        KafkaSink<Tuple2<String, String>> missingDataSink = KafkaSink.<Tuple2<String, String>>builder()
                .setKafkaProducerConfig(producerProps)
                .setRecordSerializer(missingDataSchema)
                .build();

        // FOR REQUESTS CONTAINING CORRECT VALUES
        KafkaRecordSerializationSchema<Tuple2<String, String>> processedDataSchema = KafkaRecordSerializationSchema
                .<Tuple2<String, String>>builder()
                .setTopic(processedTopic)
                .setKeySerializationSchema(new Tuple2StringKeySchema())
                .setValueSerializationSchema(new Tuple2StringValueSchema())
                .build();

        KafkaSink<Tuple2<String, String>> processedDataSink = KafkaSink.<Tuple2<String, String>>builder()
                .setKafkaProducerConfig(producerProps)
                .setRecordSerializer(processedDataSchema)
                .build();

        // ------------- CONVERT STREAM TO TABLE AND ADD INTO NEW ROW ----------------

        try {
            Table inputTable = tenv.fromDataStream(userDataStream);
            tenv.createTemporaryView("InputTable", inputTable);
            // ------------- QUERY ----------------
            StringBuilder queryBuilder = new StringBuilder("SELECT * FROM InputTable WHERE ");
            for (int i = 0; i < matchers.getFields().size(); i++) {
                String field = matchers.getFields().get(i);
                String operator = matchers.getOperators().get(i);
                String value = matchers.getValues().get(i);
                String combinator = matchers.getCombinator();

                queryBuilder.append("JSON_VALUE(f0, \'$.").append(field + "\') ").append(operator + " ")
                        .append("\'" + value + "\'");
                if (i < matchers.getFields().size() - 1) {
                    queryBuilder.append(" " + combinator + " ");
                }
            }
            // System.out.println("SQL QUERY : " + queryBuilder);
            LOG.debug("SQL QUERY : {}", queryBuilder);

            // -------------RUN THE QUERY AND THEN CONVERT TABLESTREAM TO DATASTREAM AGAIN
            // ----------------
            Table resultTable = tenv.sqlQuery(queryBuilder.toString());

            DataStream<Row> resultStream = tenv.toDataStream(resultTable);

            TypeInformation<Tuple2<String, String>> tupleTypeInfo = TypeExtractor
                    .getForObject(new Tuple2<>("key", "value"));

            DataStream<Tuple2<String, String>> processedDataStream = resultStream
                    .map(row -> row.toString().substring(3))
                    .map(rawData -> {
                        try {

                            // Parse raw data as JSON
                            ObjectMapper objectMapper = new ObjectMapper();
                            JsonNode jsonNode = objectMapper
                                    .readTree(String.valueOf(rawData));

                            JsonNode dataNode = jsonNode.get("data");//changed


                            LOG.info("dataNode value: {}", dataNode);
                           // LOG.info("datanote value: " + dataNode);//added to print datanode in taskmanager logs

                            // flag
                            int flag = 0;

                            JsonNode keyValue = dataNode.at(keyField);//changed
                           // JsonNode keyValue = jsonNode.at(keyField);

                           // JsonNode fieldValue = dataNode.at(keyField);


                            //System.out.println("printing produced values");
                            //System.out.println("produced values" + dataNode);
                            //System.out.println("printing produced values");
                            // Process data according to the template
                            StringBuilder processedKeyBuilder = new StringBuilder();
                            JSONObject processedKeyJson = new JSONObject();
                            JSONObject processedValueJson = new JSONObject();

                            System.out.println( "processed value: " + processedValueJson);//added

                            String keyName = keyField
                                    .substring(keyField.lastIndexOf("/") + 1);

                            if (keyValue != null && !keyValue.isMissingNode()
                                    && flag == 0) {
                                processedKeyBuilder.append(keyValue);
                            }


                            else {
                                processedKeyJson.put("ERROR : ", keyName + "MISSING");
                                flag = 1;
                            }

                            // Process value fields
                            if (flag == 0) {

                                // STATIC PART
                                for (int i = 0; i < staticFields.size(); i++) {
                                    processedValueJson.put(staticFields.get(i),
                                            staticValues.get(i));
                                }

                                // DYNAMIC PART

                                for (String fieldPath : fields) {
                                    String fieldName = fieldPath
                                            .substring(fieldPath
                                                    .lastIndexOf("/")
                                                    + 1);
                                    JsonNode fieldValue = jsonNode.at(fieldPath);

                                    if (fieldValue != null
                                            && !fieldValue.isMissingNode()
                                            && flag == 0) {
                                        // RETURNS THE JSON BODY IF THE TEMPLATE
                                        // ASKS
                                        // FOR JSON
                                        // BODY
                                        if (fieldValue.isObject()) {
                                            processedValueJson.put(
                                                    fieldName,
                                                    fieldValue);
                                        }

                                        // RETURNS THE VALUE IF THE TEMPLATE
                                        // ASKS FOR
                                        // VALUE
                                        else {
                                            String fieldValueString = fieldValue
                                                    .asText()
                                                    .replaceAll("\"",
                                                            "");
                                            processedValueJson.put(
                                                    fieldName,
                                                    fieldValueString);

                                        }
                                    } else {

                                        // Handle missing field value
                                        processedValueJson.put("ERROR",
                                                fieldPath.replaceAll(
                                                        "/",
                                                        ".")
                                                        + "MISSING");

                                        processedValueJson.put("DATA", rawData);
                                        flag = 1;
                                        break;
                                    }

                                }


                            }

                            return new Tuple2<>(processedKeyBuilder.toString(),
                                    processedValueJson.toString());
                        } catch (Exception e) {
                            LOG.error("An error occurred while processing data: {}",
                                    rawData, e);
                            return null;
                        }

                    }, tupleTypeInfo);

            // ------- SENDING DATA TO THE TOPIC -------------

            // SENDS THE DATA TO REQUESTED-DATA TOPIC(PROVIDED ALL FIELDS EXISTS)
            processedDataStream
                    .filter(data -> !data.f1.contains("MISSING"))

                    .sinkTo(processedDataSink);

            // SENDS THE DATA TO MISSING-DATA TOPIC(PROVIDED ANY ONE OF FIELDS DOESN'T
            // EXISTS)
            processedDataStream
                    .filter(data -> data.f1.contains("MISSING"))

                    .sinkTo(missingDataSink);

            // ------------- PRINT IN LOG LIST ----------------
            resultStream.print();

            //debug
            System.out.println("precheck");
            System.out.println("printing produced values");
            //System.out.println("produced values: " );

            env.execute();

            //debug
            System.out.println("postcheck");
        } catch (Exception e) {
            LOG.error("An error occurred while making flink sql query", e);
        }
    }

}
