package com.github.asolimando.trap17.analysis

import java.io.File
import java.sql.Timestamp

import com.github.asolimando.trap17._
import com.github.asolimando.trap17.analysis.window.WindowHelper
import com.github.asolimando.trap17.analysis.sessionization.{Event, Sessionization, Trip}
import com.github.asolimando.trap17.etl.ETL
import com.github.asolimando.trap17.graph.{GateProperty, HighwayGraph, SegmentProperty}
import org.apache.spark.ml.clustering.{GaussianMixture, GaussianMixtureModel, KMeans, KMeansModel}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import scala.collection.mutable

/**
  * Created by ale on 17/12/17.
  */
object TRAPSpark extends Helper with Sessionization with ETL with WindowHelper {

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

    df = df.filter(//month(col("timestamp")) === 1 and
      col("plate") <= 10000)


    /********* LOAD DATASET GATES AND SEGMENTS *******************/

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

    /********* GRAPH HANDLING *******************/
/*    val gates: RDD[(VertexId, (Double, Int))] =
      gfrom.rdd.map(r => (r.getInt(0).toLong, (r.getDouble(1), r.getInt(2))))

    // Create an RDD for edges
    val highwaySegments: RDD[Edge[SegmentProperty]] = arcsDF.rdd.map(a =>
      Edge(a.getInt(), SegmentProperty(a.getBoolean(0), a.getBoolean(1), a.getBoolean(2)))
    )

    // Dummy gate for segments with a missing extreme
    val dummyGate = (0, -1.0, -1.0)
    // Build the initial Graph
    val highwayGraph = Graph(gates, highwaySegments, dummyGate)

    highwayGraph.
*/
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

    val gatesPairsWithStartTimeUDF = udf{
      (gates: mutable.WrappedArray[Int], timestamps: mutable.WrappedArray[Timestamp]) =>
        gates.sliding(2).map(b => (b(0), b(1))).toList.zip(timestamps.init)
    }

    var arcsByPlateTrip = sessionized.groupBy("plate", "trip_id")
                        .agg(
                          collect_list("gate").as("arcs"),
                          collect_list("timestamp").as("timestamps")
                        )
                        .filter(size(col("arcs")) > 1)

    arcsByPlateTrip.show()

    arcsByPlateTrip = arcsByPlateTrip
      .withColumn("timedarcs", gatesPairsWithStartTimeUDF(col("arcs"), col("timestamps")))
      .selectExpr("plate", "trip_id", "explode(timedarcs) as timedarc")
      .select(
        col("plate"),
        col("trip_id"),
        col("timedarc._1._1").as("gatefrom"),
        col("timedarc._1._2").as("gateto"),
        col("timedarc._2").as("timestamp")
      )

    arcsByPlateTrip.show()
    arcsByPlateTrip.printSchema()

    def joinArcExtraInfo(df: DataFrame, arcsDF: DataFrame): DataFrame = {
      val regularArcsJoinCond = df("gatefrom") === arcsDF("gatefrom") and df("gateto") === arcsDF("gateto")
      val reversedArcsJoinCond = df("gateto") === arcsDF("gatefrom") and df("gatefrom") === arcsDF("gateto")

      df.join(broadcast(arcsDF), regularArcsJoinCond || reversedArcsJoinCond, "leftouter")
        .withColumn("posfrom", when(regularArcsJoinCond, arcsDF("pos_from")).otherwise(arcsDF("pos_to")))
        .withColumn("posto", when(regularArcsJoinCond, arcsDF("pos_to")).otherwise(arcsDF("pos_from")))
        .withColumn("highwayidfrom", when(regularArcsJoinCond, arcsDF("highwayid_from")).otherwise(arcsDF("highwayid_to")))
        .withColumn("highwayidto", when(regularArcsJoinCond, arcsDF("highwayid_to")).otherwise(arcsDF("highwayid_from")))
        .drop(arcsDF("gatefrom"))
        .drop(arcsDF("gateto"))
        .drop("pos_from")
        .drop("pos_to")
        .drop("highwayid_from")
        .drop("highwayid_to")
    }

    /*********  ARC FREQUENCY OPTIONAL STATS  *************/
    var arcsFreq = arcsByPlateTrip
      .select("gatefrom", "gateto").rdd
      .map(r => (r.getInt(0), r.getInt(1)))
      .map(p => (p, 1))
      .reduceByKey(_ + _)
      .map(r => (r._1._1, r._1._2, r._2))
      .toDF("gatefrom", "gateto", "count")
      .orderBy(desc("count"))

    arcsFreq = joinArcExtraInfo(arcsFreq, arcsDF)

    arcsFreq.orderBy(desc("count")).show()

    saveCSV(arcsFreq, "arcs_" + CUT_TIME + "m")
    /*****************************************************/

    arcsByPlateTrip = joinArcExtraInfo(arcsByPlateTrip, arcsDF)

    arcsByPlateTrip.show()
    arcsByPlateTrip.printSchema()

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