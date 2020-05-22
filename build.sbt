name := "Concepts"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" % "spark-mllib_2.11" % sparkVersion,
  "org.apache.hadoop" % "hadoop-aws" % "3.0.0",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.0",
  "com.crealytics" %% "spark-excel" % "0.8.2",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided",
  "com.typesafe.play" %% "play-json" % "2.7.0",
  "org.apache.kafka" % "kafka-clients" % "0.11.0.1"
)