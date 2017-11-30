import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.File

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature._

trait Helper {
  val CSV_INPUT = "data/all.csv"
  val PARQUET_DATA = "data/raw.parquet"

  val CONFLICTS_DATA = "data/conflicts.parquet"

  val MULTINAT_DATA = "data/multinat.parquet"

  val FIXABLE_MULTINAT_DATA = "data/fixablemultinat.parquet"

  val FIXED_DATA = "data/fixed.parquet"

  val WHLOCATION = "spark-warehouse"
  val LOCALDIR = "tmpdir"

  val FEATURES_COLNAME = "features"
}

object TRAPSpark extends Helper {

  def getRawData(spark: SparkSession): DataFrame ={
    val parquetFile = new File(PARQUET_DATA)

    if(!parquetFile.isDirectory){
      val csv = spark.read
        .option("inferSchema", true)
        .option("header", false)
        .option("mode","FAILFAST")
        .option("delimiter",";")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .csv(CSV_INPUT)
        .toDF("plate", "gate", "lane", "timestamp", "nationality")
        .distinct

      csv.write.parquet(PARQUET_DATA)
    }

    readParquet(spark, PARQUET_DATA)
  }

  def writeParquet(df: DataFrame, path: String) = df.write.parquet(path)

  def readParquet(spark: SparkSession, path: String): DataFrame = spark.read.parquet(path)

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

  def getConflictsDF(spark: SparkSession, df: DataFrame): DataFrame ={
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
    if(new File(FIXABLE_MULTINAT_DATA).isDirectory)
      readParquet(spark, FIXABLE_MULTINAT_DATA)
    else {
      val res = df.filter(col("num_nat") === 2 && array_contains(col("nats"),"?"))
        .withColumn("real_nat", col("nats")(1))
        .select("plate", "real_nat")

      writeParquet(res, FIXABLE_MULTINAT_DATA)

      res
    }
  }

  def getFixedData(spark: SparkSession, df: DataFrame, fixableMultiNat: DataFrame, path: String) = {
    if(new File(FIXED_DATA).isDirectory)
      readParquet(spark, FIXED_DATA)
    else {
      val totRows = df.count

      println(df.filter(col("nationality") === "?").count + "/" + totRows + " entries with unknown nationalities")

      val fixed = df.join(fixableMultiNat, df("plate") === fixableMultiNat("plate"))
        .withColumn("nationality", col("real_nat"))

      println(fixed.filter(col("nationality") === "?").count + "/" + totRows +
        " entries with unknown nationalities after sanitization")

      writeParquet(fixed, FIXED_DATA)

      fixed
    }
  }

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("TRAP2017")
      .config("spark.sql.warehouse.dir", WHLOCATION)
      .config("spark.local.dir", LOCALDIR)
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = getRawData(spark)
      .filter(col("nationality").isNotNull && length(col("nationality")) <= 3)
//      .withColumn("timestamp", unix_timestamp(col("timestamp")))
      .cache

    df.show(false)

    val totRows = df.count

    println("Tot rows: " + totRows)

    val conflicts = getConflictsDF(spark, df)

    println(conflicts.count)
    conflicts.show()

    val multiNat = getMultinatDF(spark, df)

    println(multiNat.count)

    val fixableMultiNat = getFixableMultiNatDF(spark, multiNat)

    val fixed = getFixedData(spark, df, fixableMultiNat, FIXED_DATA)

    /*
    val plateDistinctGates = df.groupBy("plate").agg(countDistinct("gate", "lane"))

    plateDistinctGates.show(false)
*/
    val nationalityCount = df.groupBy("nationality").count

    nationalityCount.show(false)
   /*
    val formula = new RFormula()
      .setFormula("label ~ .")
      .setFeaturesCol(FEATURES_COLNAME)

    val vetorized = formula.fit(df).transform(df)
    vetorized.show(false)

    computeKMeans(vetorized, spark)
    */
  }

  def computeKMeans(df: DataFrame, spark: SparkSession) = {
    val numClusters = 2
    val numIterations = 20
    val kmeans = new KMeans()
      .setFeaturesCol(FEATURES_COLNAME)
      .setMaxIter(numIterations)
      .setK(numClusters)
      .fit(df)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val clusters = kmeans.transform(df)
    println("Cost = " + kmeans.computeCost(df) +
      "\nSummary = " + kmeans.summary.clusterSizes.mkString(", "))

    // Save and load model
    kmeans.write.overwrite.save("data/model/kmeans.model")
  }

}