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

  def dateTimesToDuration(dt1: DateTime, dt2: DateTime): Duration = new Duration(dt1, dt2)
}
