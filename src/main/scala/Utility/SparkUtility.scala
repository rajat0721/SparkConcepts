package utility

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkUtility {

  def getSparkSession: SparkSession ={
    SparkSession.builder()
      .config("spark.master", "local[2]")
      //.enableHiveSupport()
      .getOrCreate()
  }

  def init():SparkSession = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    getSparkSession
  }
}