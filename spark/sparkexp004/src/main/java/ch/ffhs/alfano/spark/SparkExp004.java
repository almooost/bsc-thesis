package ch.ffhs.alfano.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

/**
 * Spark Experiment 1
 * @author Samuel Alfano
 * # Reference: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#window-operations-on-event-time
 */
public class SparkExp004 {

    public static void main(String[] args) throws Exception {
        String checkpointDir = "hdfs://3.208.67.119:8020/spark";
        String checkpointDirKafka = "hdfs://3.208.67.119:8020/kafka";

        String server = "52.207.6.107";
        Integer port = 9092;
        String brokers = server + ":" + port.toString();
        String groupId = "test";
        String groupIdOther = "test-other";

        // Create the schema for the data
        StructType amazonReviewSchema = new StructType()
                .add("asin", DataTypes.StringType) // Amazon Standard identification number
                .add("unixReviewTime", DataTypes.StringType)
                .add("reviewerID", DataTypes.StringType)
                .add("overall", DataTypes.DoubleType);


        // Create Context
        SparkContext sparkContext = new SparkContext();
        sparkContext.setCheckpointDir(checkpointDir);

        // Create Spark Session for spark structured streaming
        SparkSession spark = SparkSession
                .builder()
                .sparkContext(sparkContext)
                .appName("SparkExp004")
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

        Dataset<Row> sparkDFOther = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", brokers)
                .option("subscribe", groupIdOther)
                .option("failOnDataLoss",false)
                .option("checkpointLocation",checkpointDirKafka)
                .load();

        // Check if streaming source is there
        sparkDF.isStreaming();
        sparkDFOther.isStreaming();

        // Print the schema of the data
        sparkDF.printSchema();
        sparkDFOther.printSchema();

        Dataset<Row> jsonLine = sparkDF
                .selectExpr("CAST(value AS STRING)");

        Dataset<Row> jsonLineOther = sparkDFOther
                .selectExpr("CAST(value AS STRING)");

        jsonLine.printSchema();
        jsonLineOther.printSchema();

        // Creates schema out of json data with StructTypeSchema
        // jsonLine hast to be a string!
        Dataset<Row> ratings = jsonLine
                .select(from_json(col("value"), amazonReviewSchema).as("rating"));

        Dataset<Row> ratingsOther = jsonLineOther
                .select(from_json(col("value"), amazonReviewSchema).as("ratingOther"));

        // Print new schema
        ratings.printSchema();
        ratingsOther.printSchema();

        // Get required columns and create new row
        Dataset<Row> rating = ratings
                .selectExpr("rating.unixReviewTime", "rating.asin", "rating.reviewerID", "rating.overall");
        Dataset<Row> ratingOther = ratingsOther
                .selectExpr("ratingOther.unixReviewTime","ratingOther.asin", "ratingOther.reviewerID", "ratingOther.overall");

        // Flatten rating and convert unix time to date time for windowing
        // ref: https://docs-snaplogic.atlassian.net/wiki/spaces/SD/pages/2458071/Date+Functions+and+Properties+Spark+SQL
        Dataset<Row> ratingFlattened = rating
                .withColumn("unixReviewTime", to_utc_timestamp(col("unixReviewTime"),"yyyy-MM-dd HH:mm:ss"))
                .withWatermark("unixReviewTime", "5 minutes");

        Dataset<Row> ratingFlattenedOther = ratingOther
                .withColumn("unixReviewTime", to_utc_timestamp(col("unixReviewTime"),"yyyy-MM-dd HH:mm:ss"))
                .withWatermark("unixReviewTime", "5 minutes");


        // Create window over timestamp and answer identification (asin), count per window of unixTime 2 minutes, create new sliding window all 1 minutes
        Dataset<Row> line = ratingFlattened
                .join(ratingFlattenedOther, ratingFlattenedOther.col("reviewerID").equalTo(ratingFlattened.col("reviewerID")))
                .groupBy(
                    window(ratingFlattened.col("unixReviewTime"), "8 seconds", "4 seconds"),
                    ratingFlattened.col("asin")
                ).agg(avg(ratingFlattened.col("overall")));

        line.writeStream()
                // for joins, it is only supported to append output!
                .outputMode("append")
                .format("console")
                .start()
                .awaitTermination();
    }

}
