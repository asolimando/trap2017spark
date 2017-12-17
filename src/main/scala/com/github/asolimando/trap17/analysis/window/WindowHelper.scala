package com.github.asolimando.trap17.analysis.window

import com.github.asolimando.trap17.analysis.TRAPSpark.{avgDaysDifferenceUDF, avgHoursDifferenceUDF, avgMinutesDifferenceUDF, avgSecondsDifferenceUDF}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by ale on 17/12/17.
  */
class WindowHelper {

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

}
