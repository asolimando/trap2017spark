package com.github.asolimando.trap17.graph

case class GateProperty(val gateid: Int,
                        val highwayid: Int)

case class SegmentProperty(val hasEntryExit: Boolean,
                           val hasTollbooth: Boolean,
                           val hasServiceArea: Boolean)

/**
  * Created by ale on 17/12/17.
  */
class HighwayGraph {

}
