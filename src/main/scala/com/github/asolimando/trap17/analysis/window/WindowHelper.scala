package com.github.asolimando.trap17.analysis.window

import com.github.asolimando.trap17.analysis.TRAPSpark._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by ale on 17/12/17.
  */
trait WindowHelper {

  def windowAnalysis(df: DataFrame,
                     timespan: String = "1 hour",
                     gateColName: String = "gate",
                     timestampColName: String = "timestamp") = {
    val windowDF = df
      .groupBy(col("plate"), window(df.col(timestampColName), timespan))
      .agg(
        countDistinct(gateColName).as("hourly_gates"),
//        avgDaysDifferenceUDF(sort_array(collect_list(timestampColName))).as("day_avg_diff"),
//        avgHoursDifferenceUDF(sort_array(collect_list(timestampColName))).as("hour_avg_diff"),
        avgMinutesDifferenceUDF(sort_array(collect_list(timestampColName))).as("min_avg_diff")
//        ,
//        avgSecondsDifferenceUDF(sort_array(collect_list(timestampColName))).as("sec_avg_diff")
      )
      .drop("window")
      .groupBy("plate")
      .agg(
        avg("hourly_gates").as("avg_hourly_gates"),
//        avg("day_avg_diff").as("day_avg_diff"),
//        avg("hour_avg_diff").as("hour_avg_diff"),
        avg("min_avg_diff").as("min_avg_diff")
//        ,
//        avg("sec_avg_diff").as("sec_avg_diff")
      )

    windowDF
  }

}
