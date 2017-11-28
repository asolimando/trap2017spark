name := "trap2017spark"

version := "1.0"

scalaVersion := "2.11.8"

val SPARK_VERSION = "2.2.0"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SPARK_VERSION,
  "org.apache.spark" %% "spark-sql" % SPARK_VERSION,
  "org.apache.spark" %% "spark-hive" % SPARK_VERSION,
  "org.apache.spark" %% "spark-mllib" % SPARK_VERSION
)