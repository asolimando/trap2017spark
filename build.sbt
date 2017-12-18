name := "trap2017spark"

version := "1.0"

scalaVersion := "2.11.8"

val SPARK_VERSION = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SPARK_VERSION,
  "org.apache.spark" %% "spark-sql" % SPARK_VERSION,
  "org.apache.spark" %% "spark-hive" % SPARK_VERSION,
  "org.apache.spark" %% "spark-mllib" % SPARK_VERSION,
  "org.apache.spark" %% "spark-graphx" % SPARK_VERSION,

  "joda-time" % "joda-time" % "2.9.4",
  "org.joda" % "joda-convert" % "1.2",

  "org.scalanlp" %% "breeze" % "0.13.2",
  "org.scalanlp" %% "breeze-natives" % "0.13.2",
  "org.scalanlp" %% "breeze-viz" % "0.13.2"
)