package com.github.asolimando.trap17.analysis.time

import java.sql.Timestamp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.joda.time.{DateTime, Duration}

import scala.annotation.tailrec

/**
  * Created by ale on 17/12/17.
  */
trait TimeHandling extends Serializable {

  val secDurationUDF: UserDefinedFunction = {
    udf((dt1: Timestamp, dt2: Timestamp) =>
      dateTimesToDuration(new DateTime(dt1), new DateTime(dt2)).getStandardSeconds)
  }

  /**
    * Returns an UDF computing the average of a sequence of durations.
    * @param diff a function extracting the duration (at the sought temporal granularity)
    * @return an UDF computing the average of a sequence of durations.
    */
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

  /**
    * Returns a list of time differences between consecutive times.
    * @param dates the dates to be analyzed.
    * @param diff the differencing function.
    * @return a list of time differences between consecutive times.
    */
  def datesDifference(dates: List[DateTime], diff: Duration => Long): List[Long] = {
    @tailrec
    def iter(dates: List[DateTime], acc: List[Long]): List[Long] = dates match {
      case Nil => acc
      case a :: Nil => acc
      case a :: b :: tail => iter(b :: tail, diff(new Duration(a, b)) :: acc)
    }
    iter(dates, Nil)
  }

  /**
    * Converting a pair of datetime to the equivalent duration between them.
    * @param dt1 the first datetime
    * @param dt2 the second datetime
    * @return the duration between two datetimes.
    */
  def dateTimesToDuration(dt1: DateTime, dt2: DateTime): Duration = new Duration(dt1, dt2)
}
