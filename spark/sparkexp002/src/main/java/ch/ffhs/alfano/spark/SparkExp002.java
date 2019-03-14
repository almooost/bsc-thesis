package ch.ffhs.alfano.spark;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

/**
 * Spark Experiment 2
 * @author Samuel Alfano
 * # Reference: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time
 */
public class SparkExp002 {

    public static void main(String[] args) throws Exception {
        String checkpointDir = "hdfs://3.208.67.119:8020/spark";
        String checkpointDirKafka = "hdfs://3.208.67.119:8020/kafka";

        String server = "52.207.6.107";
        Integer port = 9092;
        String brokers = server + ":" + port.toString();
        String groupId = "test";
        Collection<String> topics = Arrays.asList("test");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Create the schema for the data
        StructType amazonReviewSchema = new StructType()
                .add("asin", DataTypes.StringType) // Amazon Standard identification number
                .add("unixReviewTime", DataTypes.IntegerType)
                .add("reviewerID", DataTypes.StringType)
                .add("overall", DataTypes.DoubleType);

        // Create Context
        SparkContext sparkContext = new SparkContext();
        sparkContext.setCheckpointDir(checkpointDir);

        // Create Spark Session for spark structured streaming
        SparkSession spark = SparkSession
                .builder()
                .sparkContext(sparkContext)
                .appName("SparkExp002")
                .getOrCreate();

        // Read test from socket, creates a DataFrame for the streaming data
        Dataset<Row> sparkDF = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", brokers)
                .option("subscribe", groupId)
                .option("failOnDataLoss",false)
                .option("checkpointLocation",checkpointDirKafka)
                .load();

        // Check if streaming source is there
        sparkDF.isStreaming();

        // Print the schema of the data
        sparkDF.printSchema();

        Dataset<Row> jsonLine = sparkDF
                .selectExpr("CAST(value AS STRING)");

        jsonLine.printSchema();

        // Creates schema out of json data with StructTypeSchema
        // jsonLine has to be a string!
        Dataset<Row> ratings = jsonLine
                .select(from_json(col("value"), amazonReviewSchema).as("rating"));

        // Print new schema
        ratings.printSchema();

        // Get required columns and create new row
        Dataset<Row> rating = ratings.selectExpr("rating.unixReviewTime", "rating.asin", "rating.reviewerID", "rating.overall");

        // Flatten rating and convert unix time to date time for windowing
        // ref: https://docs-snaplogic.atlassian.net/wiki/spaces/SD/pages/2458071/Date+Functions+and+Properties+Spark+SQL
        Dataset<Row> ratingFlattened = rating.withColumn("unixReviewTime", from_unixtime(col("unixReviewTime"),"yyyy-MM-dd HH:mm:ss"));

        // Create window over timestamp and answer identification (asin), count per window of unixTime 2 minutes, create new sliding window all 1 minutes
        Dataset<Row> line = ratingFlattened
                .groupBy(
                        window(ratingFlattened.col("unixReviewTime"), "6 minutes", "3 minutes"),
                        ratingFlattened.col("reviewerID")
                ).agg(avg("overall"));

        // write output to console (debuging, remove because of memory issues) update result
        line.writeStream()
                .outputMode("update")
                .format("console")
                .start()
                .awaitTermination();

    }

}
