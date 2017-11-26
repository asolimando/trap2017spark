import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.File

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature._

trait Helper {
  val CSV_INPUT = "data/all.csv"
  val PARQUET_DATA = "data/raw.parquet"

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

      csv.write.parquet(PARQUET_DATA)
    }

    spark.read.parquet(PARQUET_DATA)
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
      .withColumn("timestamp", unix_timestamp(col("timestamp")))
      .cache

    df.show(false)

    println("Tot rows: " + df.count)

    val multiNat = df.groupBy("plate").agg(countDistinct("nationality").as("num_nat")).filter(col("num_nat") > 1)

    println(multiNat.count)

    multiNat.filter(!array_contains(col("nats"),"?")).show
//    df.describe().show()

    val plateDistinctGates = df.groupBy("plate").agg(countDistinct("gate", "lane"))

    plateDistinctGates.show(false)

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