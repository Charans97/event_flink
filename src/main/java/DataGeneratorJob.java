import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.InputStream;
import java.util.Properties;

public class DataGeneratorJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Produce data and send to the topic and read in flink

        Properties producerConfig = new Properties();
        try (InputStream stream = DataGeneratorJob.class.getClassLoader()
                .getResourceAsStream("producer.properties")) {
            producerConfig.load(stream);
        }

// SNIPPET IF GENERATING DATA TO TOPIC AND CONSUMING FROM THE TOPIC TO FLINK


        // DataGeneratorSource<GenerateData> userdataSource =
        // new DataGeneratorSource<>(
        // index -> DataGenerator.generateData(),
        // 20,
        // RateLimiterStrategy.perSecond(1),
        // Types.POJO(GenerateData.class)
        // );

        // DataStream<GenerateData> userdataStream = env
        //                 .fromSource(userdataSource, WatermarkStrategy.noWatermarks(), "userdata_source");

        // KafkaRecordSerializationSchema<GenerateData> userdataSerializer = KafkaRecordSerializationSchema
        //                 .<GenerateData>builder()
        //                 .setTopic("userdata")
        //                 .setValueSerializationSchema(new JsonSerializationSchema<>(DataGeneratorJob::getMapper))
        //                 .build();

        // KafkaSink<GenerateData> userdataSink = KafkaSink.<GenerateData>builder()
        //                 .setKafkaProducerConfig(producerConfig)
        //                 .setRecordSerializer(userdataSerializer)
        //                 .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        //                 .build();

        // userdataStream
        //                 .sinkTo(userdataSink)
        //                 .name("userdata_sink");


// SNIPPET IF JUST CONSUMING DATA FROM TOPIC TO FLINK

// PROPERTIES FOR SOURCE TOPIC

        KafkaSource<String> userdataSource = KafkaSource.<String>builder()
                .setProperties(producerConfig)
                .setTopics("log-data")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();


// CREATE A ENVIRONMENT

        DataStream<String> userdataStream = env
                .fromSource(userdataSource, WatermarkStrategy.noWatermarks(), "userdata_source");

        KafkaRecordSerializationSchema<String> userdataSerializer = KafkaRecordSerializationSchema
                .<String>builder()
                .setTopic("userdata1")
                .setValueSerializationSchema(new JsonSerializationSchema<String>(DataGeneratorJob::getMapper))
                .build();

        KafkaSink<String> userdataSink = KafkaSink.<String>builder()
                .setKafkaProducerConfig(producerConfig)
                .setRecordSerializer(userdataSerializer)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        userdataStream
                .sinkTo(userdataSink)
                .name("userdata_sink");

        env.execute("InputStreams");
    }

    private static ObjectMapper getMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }
}
