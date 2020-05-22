import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import utility.SparkUtility

object Main {

  println("hi")
  def main(args: Array[String]): Unit = {
    println("hello")
    SparkUtility.init()
    val spark = SparkUtility.getSparkSession
    /*import spark.implicits._
    val df = Seq("1","2").toDF("a")
    df.show(false)*/

    Kafka.KafkaExample1(spark)
  }

}
