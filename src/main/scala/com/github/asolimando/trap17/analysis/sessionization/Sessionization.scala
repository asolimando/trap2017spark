package com.github.asolimando.trap17.analysis.sessionization

import java.sql.Timestamp

import com.github.asolimando.trap17.Helper
import com.github.asolimando.trap17.analysis.time.TimeHandling
import org.apache.spark.sql.Row
import org.joda.time.DateTime

case class Event(plate: Int, gate: Int, lane: Double, timestamp: Timestamp)
case class Trip(events: Seq[Event])

/**
  * Created by ale on 17/12/17.
  */
trait Sessionization extends Helper with TimeHandling {

  /**
    * Converts a row into an event.
    * @param r the row to convert
    * @return an event encoding the information of the row.
    */
  def rowToEvent(r: Row): Event =
    Event(r.getAs[Int]("plate"), r.getAs[Int]("gate"), r.getAs[Double]("lane"), r.getAs[Timestamp]("timestamp"))

  def aggregateTrip[T, U](split: (T,T) => Boolean, aggregator: (Seq[T] => U))(events: Iterator[T]): Iterator[U] = {

    type State = (Option[T], Vector[Seq[T]], Vector[T])
    val zero: State = (None, Vector.empty, Vector.empty)

    def nextState(state: State, t: T):State = {
      state match {
        case (Some(prev), x, y) if split(prev, t) => (Some(t), x :+ y, Vector(t))
        case (_, x,y) => (Some(t),x, y :+ t)
      }
    }

    def finalize(state: State): Iterator[U] = {
      val (_,x,y) = state
      (x :+ y).map(aggregator(_)).toList.toIterator
    }

    finalize(events.foldLeft(zero)(nextState))
  }

  /**
    * Returns true iff enough time has passed in between two events to consider them unrelated.
    * @return true iff enough time has passed in between two events to consider them unrelated.
    */
  val tripSplitFunc = (e1: Event, e2: Event) =>
    dateTimesToDuration(new DateTime(e1.timestamp), new DateTime(e2.timestamp)).getStandardMinutes > CUT_TIME

  /**
    * It returns true iff the highway segment ''e1''-->''e2'' is eligible a trip splitting point.
    * @param splitEligibleSegments the set of split-compatible segments.
    * @param e1 event for segment entering
    * @param e2 event for segment exit
    * @return true iff the highway segment ''e1''-->''e2'' is eligible a trip splitting point.
    */
  def tripSplitEligibilityTest(splitEligibleSegments: Set[(Int, Int)] = Set())(e1: Event, e2: Event) =
    splitEligibleSegments.contains((e1.gate, e2.gate))

  /**
    * Splits if the condition is met and the two events are an eligible split point.
    * @param isEligible a test for split eligibility
    * @param isSplit a test for the split condition
    * @param e1 a candidate to be the last event of the current session
    * @param e2 a candidate to be the first event of the next session
    * @return true iff the condition is met and the two events are an eligible split point.
    */
  def splitFuncIfEligible(isEligible: (Event, Event) => Boolean)
                         (isSplit: (Event, Event) => Boolean)
                         (e1: Event, e2: Event): Boolean = isEligible(e1, e2) && isSplit(e1, e2)
}
