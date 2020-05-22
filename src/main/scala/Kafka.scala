import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.types._
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.flume._

object Kafka{

  def KafkaExample1(spark: SparkSession) {

    import spark.implicits._

    val mySchema = StructType(Array(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("year", IntegerType),
      StructField("rating", DoubleType),
      StructField("duration", IntegerType)
    ))

    println("Getting stream of data")
    val streamingDataFrame = spark.read.schema(mySchema).option("header", true).csv("/home/rajat/sample.csv")

    println("Publish the Stream to Kafka")
    streamingDataFrame.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("topic", "topic1")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation", "/home/rajat/kafkawrite")
      .start()

    println("Subscribe the Stream From Kafka")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topic1")
      .load()

    println("Convert Stream According to mySchema and TimeStamp")
    val df1 = df//.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
      //.select(from_json($"value", mySchema).as("data"), $"timestamp")
      //.select("data.*", "timestamp")


    println("Print the DataFrame on Console")
    df1.writeStream
      .format("console")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

  def try1(spark: SparkSession) {

    val consumerKey = ""
    val consumerSecret = ""
    val accessToken = ""
    val accessTokenSecret = ""

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    val sc = spark.sparkContext

    val filters = "modi,india,bharat".split(",")

    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    spark.readStream

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    // Get the top hashtags over the previous 60 sec window
    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map { case (topic, count) => (count, topic) }
      .transform(_.sortByKey(false))
    //TwitterUtils.createStream(ssc, None)


    val streamingInputDF = spark.readStream
      .format("kafka")
      .option("", "")
      .load
  }


}
