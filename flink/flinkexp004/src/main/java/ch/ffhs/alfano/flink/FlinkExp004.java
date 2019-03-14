package ch.ffhs.alfano.flink;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * FlinkExp004, Experiment 4
 * @author Samuel Alfano
 */
public class FlinkExp004 {

    public static void main(String[] args) throws Exception {

        // create execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Enable checkpointing every 30s
        env.enableCheckpointing(30000);

        // Set time characteristics to event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        ExecutionConfig executionConfig = env.getConfig();
        executionConfig.setAutoWatermarkInterval(1000L);

                // Set Properties
        Properties properties = new Properties();
        properties.setProperty("topic", "test");
        properties.setProperty("topic-other", "test-other");
        properties.setProperty("topic-output", "test-output");
        properties.setProperty("bootstrap.servers", "52.207.6.107:9092");
        // only required for Kafka 0.8
        properties.setProperty("zookeeper.connect", "52.207.6.107:2181");
        properties.setProperty("group.id", "test");

        Properties propertiesOther = new Properties();
        propertiesOther.setProperty("topic", "test-other");
        propertiesOther.setProperty("topic-output", "test-output");
        propertiesOther.setProperty("bootstrap.servers", "52.207.6.107:9092");
        // only required for Kafka 0.8
        propertiesOther.setProperty("zookeeper.connect", "52.207.6.107:2181");
        propertiesOther.setProperty("group.id", "test-other");

        FlinkKafkaConsumer010<String> kafkaSource = new FlinkKafkaConsumer010<String>(
                properties.getProperty("topic"),
                new SimpleStringSchema(),
                properties);

        FlinkKafkaConsumer010<String> kafkaSourceOther = new FlinkKafkaConsumer010<String>(
                propertiesOther.getProperty("topic"),
                new SimpleStringSchema(),
                propertiesOther);

        //kafkaSource.setStartFromEarliest();     // start from the earliest record possible
        //kafkaSource.setStartFromLatest();       // start from the latest record
        //kafkaSource.setStartFromTimestamp(); // start from specified epoch timestamp (milliseconds)
        kafkaSource.setStartFromGroupOffsets(); // the default behaviour

        try {

            DataStream<String> streamSource = env.addSource(kafkaSource);
            DataStream<String> streamSourceOther = env.addSource(kafkaSourceOther);

            DataStream<AmazonJsonObject> windowCountsOther = streamSourceOther
                    // Use AmazonJsonObject for every line of json
                    .flatMap(new FlatMapFunction<String, AmazonJsonObject>() {
                        @Override
                        public void flatMap(String value, Collector<AmazonJsonObject> out) {

                            try {
                                if(value.length() > 20){

                                    JsonParser jsonParser = new JsonParser();
                                    JsonObject jsonObject = jsonParser.parse(value).getAsJsonObject();

                                    if (jsonObject.has("reviewerID") &&
                                            jsonObject.has("unixReviewTime") &&
                                            jsonObject.has("asin") &&
                                            jsonObject.has("overall")) {

                                        AmazonJsonObject amazonJsonObject = new AmazonJsonObject(
                                                jsonObject.get("reviewerID").getAsString(),
                                                jsonObject.get("asin").getAsString(),
                                                jsonObject.get("unixReviewTime").getAsLong(),
                                                jsonObject.get("overall").getAsDouble()
                                        );

                                        //System.out.println("flatMap to STring: " + amazonJsonObject.toString());

                                        out.collect(amazonJsonObject);
                                    }

                                }

                            } catch (Exception e){
                                System.out.println("flatMap Exception: " + e.getMessage());
                                System.out.println("Object: " + this.toString());
                                e.printStackTrace();
                            }
                        }
                    });

            DataStream<String> windowCounts = streamSource
                    // Use AmazonJsonObject for every line of json
                    .flatMap(new FlatMapFunction<String, AmazonJsonObject>() {
                        @Override
                        public void flatMap(String value, Collector<AmazonJsonObject> out) {

                            try {
                                if(value.length() > 20){

                                    JsonParser jsonParser = new JsonParser();
                                    JsonObject jsonObject = jsonParser.parse(value).getAsJsonObject();

                                    if (jsonObject.has("reviewerID") &&
                                            jsonObject.has("unixReviewTime") &&
                                            jsonObject.has("asin") &&
                                            jsonObject.has("overall")) {

                                        AmazonJsonObject amazonJsonObject = new AmazonJsonObject(
                                                jsonObject.get("reviewerID").getAsString(),
                                                jsonObject.get("asin").getAsString(),
                                                jsonObject.get("unixReviewTime").getAsLong(),
                                                jsonObject.get("overall").getAsDouble()
                                        );

                                        //System.out.println("flatMap to STring: " + amazonJsonObject.toString());

                                        out.collect(amazonJsonObject);
                                    }

                                }

                            } catch (Exception e){
                                System.out.println("flatMap Exception: " + e.getMessage());
                                System.out.println("Object: " + this.toString());
                                e.printStackTrace();
                            }
                        }
                    })
                    .join(windowCountsOther)
                    .where(new KeySelector<AmazonJsonObject, String>() {
                        @Override
                        public String getKey(AmazonJsonObject amazonJsonObject) throws Exception {
                            System.out.println(amazonJsonObject.reviewerID);
                            return amazonJsonObject.reviewerID;
                        }
                    })
                    .equalTo(new KeySelector<AmazonJsonObject, String>() {
                        @Override
                        public String getKey(AmazonJsonObject amazonJsonObject) throws Exception {
                            return amazonJsonObject.reviewerID;
                        }
                    })
                    .window(SlidingEventTimeWindows.of(Time.seconds(8), Time.seconds(4)))
                    .trigger(CountTrigger.of(10))
                    .apply(new JoinFunction<AmazonJsonObject, AmazonJsonObject, String>() {
                        @Override
                        public String join(AmazonJsonObject amazonJsonObject, AmazonJsonObject amazonJsonObject2) throws Exception {
                            return amazonJsonObject.asin + "," + amazonJsonObject2.asin + ", overall avg: "
                                    + ( (amazonJsonObject.overall + amazonJsonObject2.overall) /2 );
                        }
                    });

            windowCounts.print();

        } catch (Exception e) {
            System.out.println("main function, addSource exception");
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

        env.execute("FlinkExp004");
    }

}
