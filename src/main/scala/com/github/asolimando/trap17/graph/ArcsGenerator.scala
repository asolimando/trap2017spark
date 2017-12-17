package com.github.asolimando.trap17.graph

import com.github.asolimando.trap17.Helper
import org.apache.spark.sql.functions._

/**
  * Created by ale on 15/12/17.
  */
object ArcsGenerator extends Helper {

  case class Arc(from: Int, to: Int, entryExit: Boolean, toll: Boolean, serviceArea: Boolean)

  def main(args: Array[String]): Unit = {
    val spark = init()

    var arcsDF = readCSV(spark, "data/arcs.csv").cache

    var arcs = scala.collection.mutable.Set[Arc]()

    arcsDF.collect.map(a =>
      Arc(a.getInt(0),
          a.getInt(1),
          a.getBoolean(2),
          a.getBoolean(3),
          a.getBoolean(4))).foreach(arcs += _)

    var prevSize = -1L

    def combine(a1: Arc, a2: Arc): Option[Arc] = {
      if(a1.to == a2.from)
        Option(
          Arc(
            a1.from,
            a2.to,
            a1.entryExit || a2.entryExit,
            a1.toll || a2.toll,
            a1.serviceArea || a2.serviceArea
          )
        )
      else
        None
    }

    var fromMap = arcs.groupBy(_.from)

    while(arcs.size != prevSize){
      println(arcs.size)

      prevSize = arcs.size

      val newArcs =
        (for { a <- arcs; b <- fromMap.get(a.to).getOrElse(Set()) } yield combine(a, b))
          .filter(_.isDefined)
          .map(_.get)
          .toSeq

      newArcs.foreach{a =>
        arcs += a
        fromMap.get(a.from).get += a
      }
    }

    import spark.sqlContext.implicits._

    val arcsClosure =
      spark.sparkContext.parallelize(arcs.toList)
      .map(a => (a.from, a.to, a.entryExit, a.toll, a.serviceArea))
      .toDF("gatefrom", "gateto", "hasEntryExit", "hasTollbooth", "hasServiceArea")

    //arcsDF.orderBy(col("gatefrom"), col("gateto")).show(200, false)

    arcsClosure.orderBy(col("gatefrom"), col("gateto")).show(200, false)

    saveCSV(arcsClosure, "data/arcs_closure.csv")
  }
}
