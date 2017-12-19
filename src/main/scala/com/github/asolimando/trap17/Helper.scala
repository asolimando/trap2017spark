package com.github.asolimando.trap17

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by ale on 17/12/17.
  */
trait Helper {
  val CSV_INPUT = "data/all.csv"
  val RAW_DATA = "data/raw.parquet"

  val CONFLICTS_DATA = "data/conflicts.parquet"

  val MULTINAT_DATA = "data/multinat.parquet"

  val FIXABLE_MULTINAT_DATA = "data/fixablemultinat.parquet"

  val FIXED_DATA = "data/fixed.parquet"

  val WHLOCATION = "spark-warehouse"
  val LOCALDIR = "tmpdir"

  val FEATURES_COLNAME = "features"

  val GATES_DATA = "data/gates.csv"
  val ARCS_DATA = "data/arcs_closure.csv"

  val CUT_TIME = 30 // minutes

  def writeParquet(df: DataFrame, path: String) = df.write.parquet(path)

  def readParquet(spark: SparkSession, path: String): DataFrame = spark.read.parquet(path)

  def readCSV(spark: SparkSession, path: String): DataFrame =
    spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv(path)

  def saveCSV(df: DataFrame, path: String) =
    df.coalesce(1).write.mode(SaveMode.Overwrite).option("header", true).csv(path)

  def init(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName("TRAP2017")
      .config("spark.sql.warehouse.dir", WHLOCATION)
      .config("spark.local.dir", LOCALDIR)
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark
  }
}
