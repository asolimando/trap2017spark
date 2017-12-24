package com.github.asolimando.trap17.etl

import java.io.File
import java.sql.Timestamp

import com.github.asolimando.trap17.Helper
import com.github.asolimando.trap17.analysis.sessionization.{Event, Sessionization, Trip}
import com.github.asolimando.trap17.analysis.window.WindowHelper
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.joda.time.DateTime

import scala.collection.mutable

/**
  * Created by ale on 17/12/17.
  */
trait ETL extends Helper with Sessionization with WindowHelper {

  /**
    * Loading raw data
    * @param spark spark session
    * @return a dataframe with the raw data
    */
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

  /**
    * Returns the dataframe with the information associated to plates having different nationalities in the dataset.
    * @param spark the spark session
    * @param df the data to process
    * @return the dataframe with the information associated to plates having different nationalities in the dataset.
    */
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

  /**
    * Returns the dataframe containing spatial conflicts, that is, events for multiple vehicles at the same place and time.
    * @param spark the spark session
    * @param df the data to process
    * @return the dataframe containing spatial conflicts, that is, events for multiple vehicles at the same place and time.
    */
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

  /**
    * Returns the subset of the data for different plates having multiple nationalities associated that can be fixed without
    * ambiguity (that is, at most two distinct nationalities, one of them ''?'')
    * @param spark the spark session
    * @param df the data to process
    * @return subset of the data for different plates having multiple nationalities associated that can be fixed without
    * ambiguity (that is, at most two distinct nationalities, one of them ''?'')
    */
  def getFixableMultiNatDF(spark: SparkSession, df: Dataset[Row]) ={
    val res = df.filter(col("num_nat") === 2 && array_contains(col("nats"),"?"))
      .withColumn("real_nat", col("nats")(1))
      .select("plate", "real_nat")

    writeParquet(res, FIXABLE_MULTINAT_DATA)

    res
  }

  /**
    * Returns the sanitized data.
    * @param spark the spark session
    * @param df the input data to be processed
    * @param fixableMultiNat the subset of data with multiple plates that can be sanitized
    * @return the sanitized data.
    */
  def getFixedData(spark: SparkSession, df: DataFrame, fixableMultiNat: DataFrame) ={
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

  /**
    * Method applying a sequence of pipeline stages in order.
    * @param df the input data to process.
    * @param stages the sequence of stages to be applied.
    * @return the data output of the stages application.
    */
  def applyPipelineStages(df: DataFrame, stages: Seq[PipelineStage]): DataFrame ={
    val pipeline = new Pipeline().setStages(stages.toArray)
    val model = pipeline.fit(df)
    model.transform(df)
  }

  /**
    * Returns a dataframe where the numeric columns are normalized (standard scaling).
    * @param df the input data
    * @param numericCols the numeric columns
    * @return a dataframe where the numeric columns are normalized (standard scaling).
    */
  def normalize(df: DataFrame, numericCols: Seq[String]): DataFrame = {
    val vectorizeCol = udf( (v: Double) => org.apache.spark.ml.linalg.Vectors.dense(Array(v)) )
    val devectorizeCol = udf( (v: DenseVector) => v.apply(0) )

    // Vectorize columns
    var vectorizedDF =
      numericCols.foldRight(df)((s: String, df: DataFrame) => df.withColumn(s, vectorizeCol(col(s))))

    val scalers: Seq[PipelineStage] =
      numericCols.map(
        colName => new StandardScaler()
          .setInputCol(colName)
          .setOutputCol(colName + "_tmp")
          .setWithStd(true)
          .setWithMean(false)
      )

    vectorizedDF = applyPipelineStages(vectorizedDF, scalers)

    vectorizedDF = numericCols.foldRight(vectorizedDF)((s: String, df: DataFrame) =>
      df.drop(s).withColumnRenamed(s + "_tmp", s))

    // Devectorize columns
    numericCols.foldRight(vectorizedDF)((s: String, df: DataFrame) =>
      df.withColumn(s, devectorizeCol(col(s))))
  }

  /**
    * Builds a vectorized version of the dataset.
    * @param spark the spark session
    * @return a vectorized version of the dataset.
    */
  def getVectorizedDF(spark: SparkSession) ={

    if(new File(VECTORIZED_DATA).isDirectory)
      readParquet(spark, VECTORIZED_DATA)
    else {
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

          getFixedData(spark, df, fixableMultiNat)
        }

//      df = df.filter(col("plate") <= 100)

      /********* LOAD DATASET GATES AND SEGMENTS *******************/

      // enrich segments (arcs) with the information of the gates at their extremities
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

      // method definying which pair of events are eligible as split points for sessionization
      val validTripCutSegments: Set[(Int, Int)] =
        arcsDF.filter(!col("hasServiceArea") && col("hasEntryExit")).rdd.map(r => (r.getInt(0), r.getInt(1))).collect.toSet

      val soundSplitTestFunc:((Event, Event) => Boolean) =
        splitFuncIfEligible(tripSplitEligibilityTest(validTripCutSegments))(tripSplitFunc)

      val nationalityCount = df.groupBy("nationality").count
      nationalityCount.show(false)

      // sessionization in order to split the sequence of events for each car in coherent trips
      // (according to a given split criteria for understanding when a trip ends and another starts)
      df = df.repartition(col("plate"))
      df = df.sortWithinPartitions("plate", "timestamp")

      import spark.sqlContext.implicits._

      val sessionized = df.rdd.map(rowToEvent(_))
        .mapPartitions[Trip](aggregateTrip[Event, Trip](soundSplitTestFunc, (x: Seq[Event]) => new Trip(x)))
        .zipWithIndex()
        .map(r => (r._1.events, r._2))
        .toDF("trip", "tripid")
        .select(explode(col("trip")).as("event"), col("tripid"))
        .select(
          "event.plate",
          "event.gate",
          "event.lane",
          "event.timestamp",
          "tripid"
        )

      sessionized.show(false)

      val gatesPairsWithStartTimeUDF = udf{
        (gates: mutable.WrappedArray[Int], timestamps: mutable.WrappedArray[Timestamp]) =>
          gates.sliding(2).map(a => (a(0), a(1))).toList zip timestamps.sliding(2).map(a => (a(0), a(1))).toList
      }

      var arcsByPlateTrip = sessionized.groupBy("plate", "tripid")
        .agg(
          collect_list("gate").as("arcs"),
          collect_list("timestamp").as("timestamps")
        )
        .filter(size(col("arcs")) > 1)

      arcsByPlateTrip.show(false)

      arcsByPlateTrip = arcsByPlateTrip
        .withColumn("timedarcs", gatesPairsWithStartTimeUDF(col("arcs"), col("timestamps")))
        .selectExpr("plate", "tripid", "explode(timedarcs) as timedarc")
        .select(
          col("plate"),
          col("tripid"),
          col("timedarc._1._1").as("gatefrom"),
          col("timedarc._1._2").as("gateto"),
          col("timedarc._2._1").as("tstart"),
          col("timedarc._2._2").as("tend")
        )

      arcsByPlateTrip.show(false)
      arcsByPlateTrip.printSchema

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

      arcsFreq.orderBy(desc("count")).show(false)

      saveCSV(arcsFreq, "arcs_" + CUT_TIME + "m")
      /*****************************************************/

      arcsByPlateTrip = joinArcExtraInfo(arcsByPlateTrip, arcsDF)

      arcsByPlateTrip =
        arcsByPlateTrip
          .withColumn("segmentDist", abs(col("posto") - col("posfrom")))
          .withColumn("segmentDuration", secDurationUDF(col("tstart"), col("tend")).cast(DoubleType) / 3600.0)
          .withColumn("segmentSpeed", col("segmentDist") /  col("segmentDuration"))

      arcsByPlateTrip.show(false)
      arcsByPlateTrip.printSchema()

      // summary single trip statistics by plate
      var tripsStatsByPlate = arcsByPlateTrip.groupBy("plate", "tripid").agg(
        sum("segmentDist").as("tripDist"),
        sum("segmentDuration").as("tripDuration"),
        (sum("segmentDist") / sum("segmentDuration")).as("tripSpeed")
      )

      val timeInServiceAreaByPlateTrip = arcsByPlateTrip.filter("hasServiceArea").groupBy("plate", "tripid").agg(
        sum("segmentDuration").as("totTimeServiceArea")
      )

      tripsStatsByPlate = tripsStatsByPlate.join(
        timeInServiceAreaByPlateTrip,
        tripsStatsByPlate("plate") === timeInServiceAreaByPlateTrip("plate") &&
          tripsStatsByPlate("tripid") === timeInServiceAreaByPlateTrip("tripid")
      )
        .drop(timeInServiceAreaByPlateTrip("plate"))
        .drop(timeInServiceAreaByPlateTrip("tripid"))

      tripsStatsByPlate.show(false)

      /*
          println(df.count)

          df.columns.map(c => c -> df.na.drop(Array(c)).count()).foreach(println)

          //df = df.na.fill("?", Array("nationality"))

      //    println(df.count)

          df = df.filter(month(col("timestamp")) === 1)
          df.show(false)
      */

      /*  --- not interesting after sessionization ---
          val windowDF = windowAnalysis(arcsByPlateTrip, gateColName = "gatefrom", timestampColName = "tstart")

          windowDF.show(false)
      */

      val nullValues = df.filter(df.columns.map(col(_).isNull).reduce(_ or _))
      val anyNullCount = nullValues.count

      if(anyNullCount > 0){
        nullValues.show(false)
        println("Count: " + df.count)
        df = df.na.drop
        println("Count after dropping nulls: " + df.count)
      }

      val dayOfWeekUDF = udf((ts: Timestamp) => new DateTime(ts).dayOfWeek.getAsString.toInt)

      df = df.withColumn("month", month(col("timestamp")))
        .withColumn("day", dayofmonth(col("timestamp")))
        .withColumn("hour", hour(col("timestamp")))
        .withColumn("dayofweek", dayOfWeekUDF(col("timestamp")))

      val mostFreqValueUDF: UserDefinedFunction = udf {
        (values: mutable.WrappedArray[Int]) =>
          values.groupBy(identity).map(p => (p._1, p._2.size)).toSeq.maxBy(_._2)._1
      }

      // derive "global features" for a single plate (not considering the single trips)
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
        mostFreqValueUDF(sort_array(collect_list("hour"))).as("mostFreqHour"),
        mostFreqValueUDF(sort_array(collect_list("dayofweek"))).as("mostFreqDayOfWeek"),
        mostFreqValueUDF(sort_array(collect_list("gate"))).as("mostFreqGate")
      )

      /*
          df = df.join(windowDF, df("plate") === windowDF("plate"))
                 .drop(windowDF("plate"))
      */

      // derive features summarizing the behaviour of a single plate in terms of its trips
      val summaryByPlate = tripsStatsByPlate.groupBy("plate").agg(
        min("tripDist").as("minTripDist"),
        max("tripDist").as("maxTripDist"),
        avg("tripDist").as("avgTripDist"),

        min("tripDuration").as("minTripDuration"),
        max("tripDuration").as("maxTripDuration"),
        avg("tripDuration").as("avgTripDuration"),

        min("tripSpeed").as("minTripSpeed"),
        max("tripSpeed").as("maxTripSpeed"),
        avg("tripSpeed").as("avgTripSpeed"),

        min("totTimeServiceArea").as("minTotTimeServiceArea"),
        max("totTimeServiceArea").as("maxTotTimeServiceArea"),
        avg("totTimeServiceArea").as("avgTotTimeServiceArea"),

        countDistinct("tripid").as("numtrips")
      )

      //merge features at dataset level and trip level
      df = df.join(summaryByPlate, df("plate") === summaryByPlate("plate"))
        .drop(summaryByPlate("plate")).cache

      println("#Plates: " + df.select("plate").distinct.count)
      df.show(false)

      df = normalize(df, getNumericColumns(df).filter(!_.equals("plate")))

      df.show(false)

      // encode the dataframe using RFormula
      val formula = new RFormula()
        .setFormula("label ~ .")
        .setFeaturesCol(FEATURES_COLNAME)

      val vectorized = formula.fit(df).transform(df)

      writeParquet(vectorized, VECTORIZED_DATA)

      vectorized.cache
    }
  }

  /**
    * Returns the name of the columns in the dataframe ''df'' which are numeric.
    * @param df the input dataframe
    * @return the name of the columns in the dataframe ''df'' which are numeric.
    */
  def getNumericColumns(df: DataFrame): Seq[String] = {
    val isNumeric: PartialFunction[(String, String), String] = {
      case (name, "ByteType" |
                  "DecimalType" |
                  "DoubleType" |
                  "FloatType" |
                  "IntegerType" |
                  "LongType" |
                  "ShortType") => name
    }

    df.dtypes collect isNumeric
  }
}