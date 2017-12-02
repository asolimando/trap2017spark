import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import java.io.File
import java.sql.Timestamp

import org.apache.spark.ml.clustering.{GaussianMixture, GaussianMixtureModel, KMeans, KMeansModel}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.joda.time.{DateTime, Duration}

import scala.annotation.tailrec

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
}

object TRAPSpark extends Helper {

  def getRawData(spark: SparkSession): DataFrame ={
    val parquetFile = new File(RAW_DATA)

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

      csv.write.parquet(RAW_DATA)
    }

    readParquet(spark, RAW_DATA)
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

  def windowAnalysis(df: DataFrame, timespan: String = "1 hour") = {
    val windowDF = df
      .groupBy(col("plate"), window(df.col("timestamp"), timespan))
      .agg(
        countDistinct("gate").as("hourly_gates"),
        avgDaysDifferenceUDF(sort_array(collect_list("timestamp"))).as("day_avg_diff"),
        avgHoursDifferenceUDF(sort_array(collect_list("timestamp"))).as("hour_avg_diff"),
        avgMinutesDifferenceUDF(sort_array(collect_list("timestamp"))).as("min_avg_diff"),
        avgSecondsDifferenceUDF(sort_array(collect_list("timestamp"))).as("sec_avg_diff")
      )
      .drop("window")
      .groupBy("plate")
      .agg(
        avg("hourly_gates").as("avg_hourly_gates"),
        avg("day_avg_diff").as("day_avg_diff"),
        avg("hour_avg_diff").as("hour_avg_diff"),
        avg("min_avg_diff").as("min_avg_diff"),
        avg("sec_avg_diff").as("sec_avg_diff")
      )

    windowDF.show(false)

    windowDF
  }

  def retrieveGetAvgDurationUDF(diff: Duration => Long): UserDefinedFunction = {
    udf((reqDates: Seq[Timestamp]) => {
      val timeDiffList = datesDifference(reqDates.map(new DateTime(_)).toList, diff)

      if(timeDiffList.length == 0)
        0.0
      else
        timeDiffList.sum.toDouble / timeDiffList.length.toDouble
    })
  }

  def avgDaysDifferenceUDF = retrieveGetAvgDurationUDF((x:Duration) => x.getStandardDays)
  def avgMinutesDifferenceUDF = retrieveGetAvgDurationUDF((x:Duration) => x.getStandardMinutes)
  def avgHoursDifferenceUDF = retrieveGetAvgDurationUDF((x:Duration) => x.getStandardHours)
  def avgSecondsDifferenceUDF = retrieveGetAvgDurationUDF((x:Duration) => x.getStandardSeconds)

  def datesDifference(dates: List[DateTime], diff: Duration => Long): List[Long] = {
    @tailrec
    def iter(dates: List[DateTime], acc: List[Long]): List[Long] = dates match {
      case Nil => acc
      case a :: Nil => acc
      case a :: b :: tail => iter(b :: tail, diff(new Duration(a, b)) :: acc)
    }
    iter(dates, Nil)
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
        var df = getRawData(spark).filter(col("nationality").isNotNull && length(col("nationality")) <= 3)

        df.show(false)

        println("Tot rows: " + df.count)

        val spatialConflictsDF = getSpatialConflictsDF(spark, df)

        println("Spatial conflicts count: " + spatialConflictsDF.count)
        spatialConflictsDF.show(false)

        // remove spatial spatialConflictsDF
        df = df.except(
          df.join(spatialConflictsDF,
            df("gate") === spatialConflictsDF("gate") and
            df("lane") === spatialConflictsDF("lane") and
            df("timestamp") === spatialConflictsDF("timestamp"), "leftsemi"))

        println("Spatial conflicts removed, count: " + df.count)
/*
        println(df.filter(col("plate").isNull).count)

        println("Spatial conflicts lefts: " +
          df.groupBy("gate", "lane", "timestamp")
          .agg(countDistinct("plate").as("plates"))
          .filter(col("plates") > 1).count)
*/
        val fixableMultiNat =
          if(new File(FIXABLE_MULTINAT_DATA).isDirectory)
            readParquet(spark, FIXABLE_MULTINAT_DATA)
          else {
            val multiNat = getMultinatDF(spark, df)

            multiNat.show(false)

            println(multiNat.count)

            getFixableMultiNatDF(spark, multiNat)
          }

        fixableMultiNat.show(false)

        getFixedData(spark, df, fixableMultiNat, FIXED_DATA)
      }

    df = df.cache

/*
    println(df.count)

    df.columns.map(c => c -> df.na.drop(Array(c)).count()).foreach(println)

    //df = df.na.fill("?", Array("nationality"))

//    println(df.count)

    df = df.filter(month(col("timestamp")) === 1)
    df.show(false)
*/

    val windowDF = windowAnalysis(df)

    df.filter(col("plate").isNull).show(false)

    df.filter(df.columns.map(col(_).isNull).reduce(_ or _)).show(false)

    println("Count: " + df.count)
    df = df.na.drop
    println("Count after dropping nulls: " + df.count)

    val nationalityCount = df.groupBy("nationality").count
    nationalityCount.show(false)

    val dayOfWeekUDF = udf((ts: Timestamp) => new DateTime(ts).dayOfWeek.getAsString)

    df = df.withColumn("month", month(col("timestamp")))
           .withColumn("day", dayofmonth(col("timestamp")))
           .withColumn("hour", hour(col("timestamp")))
           .withColumn("dayofweek", dayOfWeekUDF(col("timestamp")))

    df = df.groupBy("plate").agg(
      first("nationality").as("nationality"),
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
      unix_timestamp(min("timestamp")).as("min_time"),
      unix_timestamp(max("timestamp")).as("max_time")
    )

    df = df.join(windowDF, df("plate") === windowDF("plate"))
           .drop(windowDF("plate"))

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

      def getProbPredUDF = udf((pred: Int, probs: DenseVector) => probs.values(pred))

      val avgConfidence = model.transform(test)
        .withColumn("prob_pred", getProbPredUDF(col(model.getPredictionCol), col(model.getProbabilityCol)))
        .groupBy()
        .agg(avg("prob_pred"))
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