import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import java.io.File
import java.sql.Timestamp

import org.apache.spark.ml.clustering.{GaussianMixture, GaussianMixtureModel, KMeans, KMeansModel}
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.linalg.VectorUDT
import org.joda.time.DateTime

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
        .drop(fixableMultiNat("plate"))
        .drop(fixableMultiNat("real_nat"))

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

    var df =
      if(new File(FIXED_DATA).isDirectory)
        readParquet(spark, FIXED_DATA)
      else {
        val df = getRawData(spark)
          .filter(col("nationality").isNotNull && length(col("nationality")) <= 3)
        //      .withColumn("timestamp", unix_timestamp(col("timestamp")))

        df.show(false)

        val totRows = df.count

        println("Tot rows: " + totRows)

        val conflicts = getConflictsDF(spark, df)

        println(conflicts.count)
        conflicts.show(false)

        val multiNat = getMultinatDF(spark, df)

        multiNat.show(false)

        println(multiNat.count)

        val fixableMultiNat = getFixableMultiNatDF(spark, multiNat)

        fixableMultiNat.show(false)

        getFixedData(spark, df, fixableMultiNat, FIXED_DATA)
      }

    val nationalityCount = df.groupBy("nationality").count
    nationalityCount.show(false)

    val dayOfWeekUDF = udf((ts: Timestamp) => new DateTime(ts).dayOfWeek.getAsString)

    df = df.withColumn("month", month(col("timestamp")))
           .withColumn("day", dayofmonth(col("timestamp")))
           .withColumn("hour", hour(col("timestamp")))
           .withColumn("dayofweek", dayOfWeekUDF(col("timestamp")))

    df = df.groupBy("plate", "nationality").agg(
      countDistinct("gate").as("num_gates"),
      countDistinct("lane").as("num_lanes"),
      countDistinct("gate", "lane").as("num_gatelane"),
      countDistinct("month").as("num_months"),
      countDistinct("day").as("num_days"),
      countDistinct("hour").as("num_hours"),
      countDistinct("dayofweek").as("num_daysofweek"),
      countDistinct("timestamp").as("num_seen"),
      min("month").as("min_month"),
      max("month").as("max_month"),
      min("timestamp").as("min_time"),
      max("timestamp").as("max_time")
    )

    val formula = new RFormula()
      .setFormula("label ~ .")
      .setFeaturesCol(FEATURES_COLNAME)

    val vetorized = formula.fit(df).transform(df)
    vetorized.show(false)

    computeClusters(vetorized, spark, "gmm")
    computeClusters(vetorized, spark, "kmeans")
  }

  def computeKMeans(df: DataFrame,
                        numIterations: Int = 20,
                        kVals: Seq[Int] = Seq(2, 3, 4, 5, 10, 20, 30, 40)): KMeansModel ={

    val Array(train, test) = df.randomSplit(Array(0.7, 0.3))
    val costs = kVals.map { k =>
      val model = new KMeans()
        .setK(k)
        .setMaxIter(numIterations)
        .fit(train)
      (k, model, model.computeCost(test))
    }
    println("Clustering cross-validation:")
    costs.foreach { case (k, model, cost) => println(f"WCSS for K=$k id $cost%2.2f") }

    val best = costs.minBy(_._3)
    println("Best model with k = " + best._1)

    val bestModel = best._2

    println("Cost = " + bestModel.computeCost(df) + "\n" +
      "Summary = " + bestModel.summary.clusterSizes.mkString(", "))

    bestModel
  }

  def computeGMM(df: DataFrame,
                 numIterations: Int = 20,
                 kVals: Seq[Int] = Seq(2, 3, 4, 5, 10, 20, 30, 40)): GaussianMixtureModel ={

    val Array(train, test) = df.randomSplit(Array(0.7, 0.3))
    val costs = kVals.map { k =>
      val model = new GaussianMixture()
        .setK(k)
        .setMaxIter(numIterations)
        .fit(train)

      def getProbPredUDF = udf((pred: Int, probs: Seq[Double]) => probs(pred))

      val avgConfidence = model.transform(test)
        .withColumn("prob_pred", getProbPredUDF(col(model.getPredictionCol), array(col(model.getProbabilityCol))))
        .groupBy()
        .agg(avg(model.getProbabilityCol))
        .head.getDouble(0)

      (model.summary.k, model, avgConfidence)
    }
    println("Clustering cross-validation:")
    costs.foreach { case (k, model, avgConf) => println(f"AvgConfidence for K=$k $avgConf%2.2f") }

    val best = costs.maxBy(_._3)
    println("Best model with k = " + best._1)

    val bestModel = best._2

    println("AVG Confidence= " + best._3 + "\n" +
      "Summary = " + bestModel.summary.clusterSizes.mkString(", "))

    bestModel
  }

  def computeClusters(df: DataFrame, spark: SparkSession, algo: String) = {

    val model = algo match {
      case "kmeans" => computeKMeans(df)
      case "gmm" => computeGMM(df)
    }

    model.write.overwrite.save("data/model/" + algo + ".model")
  }

}