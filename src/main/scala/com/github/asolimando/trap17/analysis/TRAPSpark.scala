package com.github.asolimando.trap17.analysis

import java.io.File
import java.sql.Timestamp

import com.github.asolimando.trap17._
import com.github.asolimando.trap17.analysis.sessionization.{Event, Sessionization, Trip}
import com.github.asolimando.trap17.etl.ETL
import org.apache.spark.ml.clustering.{GaussianMixture, GaussianMixtureModel, KMeans, KMeansModel}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime

import scala.collection.mutable

/**
  * Created by ale on 17/12/17.
  */
object TRAPSpark extends Helper with Sessionization with ETL {

  def main(args: Array[String]) {

    val spark = init()

    // basic ETL for trying to derive missing nationality information, removing spatial conflicts etc
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

    df = df.filter(month(col("timestamp")) === 1 and col("plate") === 259)

    // enrich segments with the information of the gates at their extremities
    val gfrom = readCSV(spark, GATES_DATA)
      .withColumnRenamed("gateid", "gateid_from")
      .withColumnRenamed("pos", "pos_from")
      .withColumnRenamed("highwayid", "highwayid_from")

    val gto = readCSV(spark, GATES_DATA)
      .withColumnRenamed("gateid", "gateid_to")
      .withColumnRenamed("pos", "pos_to")
      .withColumnRenamed("highwayid", "highwayid_to")

    var arcsDF = readCSV(spark, ARCS_DATA)

    println("#arcs pre = " + arcsDF.count)

    arcsDF = arcsDF
      .join(gfrom, arcsDF("gatefrom") === gfrom("gateid_from"))
      .join(gto, arcsDF("gateto") === gto("gateid_to"))
      .drop(gfrom("gateid_from"))
      .drop(gto("gateid_to"))

    println("#arcs post = " + arcsDF.count)

    arcsDF.show

    df = df.cache

    println("Dataset size: " + df.count)

    val validTripCutSegments: Set[(Int, Int)] =
      arcsDF.filter(!col("hasServiceArea") && col("hasEntryExit")).rdd.map(r => (r.getInt(0), r.getInt(1))).collect.toSet

    val soundSplitTestFunc:((Event, Event) => Boolean) =
      splitFuncIfEligible(tripSplitEligibilityTest(validTripCutSegments))(tripSplitFunc)

    // sessionization in order to split the sequence of events for each car in coherent trips
    // (according to a given split criteria for understanding when a trip ends and another starts)
    df = df.repartition(col("plate"))
    df = df.sortWithinPartitions("plate", "timestamp")

    import spark.sqlContext.implicits._

    val sessionized = df.rdd.map(rowToEvent(_))
      .mapPartitions[Trip](aggregateTrip[Event, Trip](soundSplitTestFunc, (x: Seq[Event]) => new Trip(x)))
      .zipWithIndex()
      .map(r => (r._1.events, r._2))
      .toDF("trip", "trip_id")
      .select(explode(col("trip")).as("event"), col("trip_id"))
      .select(
        "event.plate",
        "event.gate",
        "event.lane",
        "event.timestamp",
        "trip_id"
      )

    sessionized.show(false)

    val gatesPairsUDF = udf((a: mutable.WrappedArray[Int]) => a.sliding(2).map(b => (b(0), b(1))).toList)

    var arcsByPlateTrip = sessionized.groupBy("plate", "trip_id")
                        .agg(collect_list("gate").as("arcs"))
                        .filter(size(col("arcs")) > 1)
    arcsByPlateTrip.show()

    arcsByPlateTrip = arcsByPlateTrip.withColumn("arcs", gatesPairsUDF(col("arcs")))
           .selectExpr("explode(arcs) as arc")
           .select(
             col("arc._1").as("gatefrom"),
             col("arc._2").as("gateto"))

    arcsByPlateTrip.show()

    arcsByPlateTrip.printSchema()

    var arcsFreq = arcsByPlateTrip.rdd
      .map(r => (r.getInt(0), r.getInt(1)))
      .map(p => (p, 1))
      .reduceByKey(_ + _)
      .map(r => (r._1._1, r._1._2, r._2))
      .toDF("gatefrom", "gateto", "count")
      .orderBy(desc("count"))

    // reverse join condition needed as still not clear if the map is one-way or not
    arcsFreq = arcsFreq.join(arcsDF,
      (arcsFreq("gatefrom") === arcsDF("gatefrom") and arcsFreq("gateto") === arcsDF("gateto")) ||
        (arcsFreq("gateto") === arcsDF("gatefrom") and arcsFreq("gatefrom") === arcsDF("gateto")),
      "leftouter"
    )
    .drop(arcsDF("gatefrom"))
    .drop(arcsDF("gateto"))

    arcsFreq.filter(col("count").isNull).show()

    saveCSV(arcsFreq, "arcs_" + CUT_TIME + "m")
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
/*
    println("Count: " + df.count)
    df = df.na.drop
    println("Count after dropping nulls: " + df.count)
*/
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
