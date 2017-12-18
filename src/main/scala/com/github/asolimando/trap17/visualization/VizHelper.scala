package com.github.asolimando.trap17.visualization

import java.awt.Color

import breeze.plot._

/**
  * Created by ale on 18/12/17.
  */
trait VizHelper {

  def visualize(x: Seq[Double], y: Seq[Double]) = {

    val f1 = Figure()

    val p = f1.subplot(0)

    p += plot(x, y)

    p.title = "Clustering cost (WCSS) at varying number of clusters (K)"

    p.xlabel = "K"
    p.ylabel = "WCSS"

    p.refresh
  }
}
