package com.github.asolimando.trap17.etl

import java.io.File

import com.github.asolimando.trap17.Helper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Created by ale on 17/12/17.
  */
trait ETL extends Helper {
  def getRawData(spark: SparkSession): DataFrame ={
    val parquetFile = new File(RAW_DATA)

    if(!parquetFile.isDirectory){
      val csv = spark.read
        .option("inferSchema", true)
        .option("header", false)
        .option("mode", "FAILFAST")
        .option("delimiter", ";")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .csv(CSV_INPUT)
        .toDF("plate", "gate", "lane", "timestamp", "nationality")
        .distinct

      csv.write.parquet(RAW_DATA)
    }

    readParquet(spark, RAW_DATA)
  }

  def getMultinatDF(spark: SparkSession, df: DataFrame): DataFrame ={
    if(new File(MULTINAT_DATA).isDirectory)
      readParquet(spark, MULTINAT_DATA)
    else {
      val res = df.groupBy("plate")
        .agg(collect_set("nationality").as("nats"), countDistinct("nationality").as("num_nat"))
        .filter(col("num_nat") > 1)
        .withColumn("nats", sort_array(col("nats")))

      writeParquet(res, MULTINAT_DATA)

      res
    }
  }

  def getSpatialConflictsDF(spark: SparkSession, df: DataFrame): DataFrame ={
    if(new File(CONFLICTS_DATA).isDirectory)
      readParquet(spark, CONFLICTS_DATA)
    else{
      val res = df.groupBy("gate", "lane", "timestamp")
        .agg(countDistinct("plate").as("plates"))
        .filter(col("plates") > 1)

      writeParquet(res, CONFLICTS_DATA)

      res
    }
  }

  def getFixableMultiNatDF(spark: SparkSession, df: Dataset[Row]) = {
    val res = df.filter(col("num_nat") === 2 && array_contains(col("nats"),"?"))
      .withColumn("real_nat", col("nats")(1))
      .select("plate", "real_nat")

    writeParquet(res, FIXABLE_MULTINAT_DATA)

    res
  }

  def getFixedData(spark: SparkSession, df: DataFrame, fixableMultiNat: DataFrame, path: String) = {
    if(new File(FIXED_DATA).isDirectory)
      readParquet(spark, FIXED_DATA)
    else {
      val totRows = df.count

      println(df.filter(col("nationality") === "?").count + "/" + totRows + " entries with unknown nationalities")

      val fixed = df.join(fixableMultiNat, df("plate") === fixableMultiNat("plate"), "leftouter")
        .withColumn("nationality", when(col("real_nat").isNotNull, col("real_nat")).otherwise(col("nationality")))
        .drop(fixableMultiNat("plate"))
        .drop(fixableMultiNat("real_nat"))

      println(fixed.filter(col("nationality") === "?").count + "/" + totRows +
        " entries with unknown nationalities after sanitization")

      writeParquet(fixed, FIXED_DATA)

      fixed
    }
  }
}
