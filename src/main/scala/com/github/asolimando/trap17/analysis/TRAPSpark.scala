package com.github.asolimando.trap17.analysis

import com.github.asolimando.trap17._
import com.github.asolimando.trap17.etl.ETL
import com.github.asolimando.trap17.visualization.VizHelper
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.clustering.{GaussianMixture, GaussianMixtureModel, KMeans, KMeansModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.io.File

/**
  * Created by ale on 17/12/17.
  */
object TRAPSpark extends Helper with ETL with VizHelper {

  def main(args: Array[String]) {

    val spark = init()

    val vectorized = getVectorizedDF(spark)

    vectorized.show(false)

    val numSamples = vectorized.count

    //computeClusters(vectorized, spark, "kmeans")
    val models = computeClusters(vectorized, spark, "gmm")

    models.foreach{
      m =>
        val clust_df = m._3.transform(vectorized)
        val clustsize: DataFrame = clust_df.groupBy("prediction").count

        clustsize.show(false)

        val clusterIDoutlier = clustsize.filter(col("count") / numSamples < 0.05)
          .select("prediction")
          .collect
          .map(r => r.getInt(0))

        val anomalies = clust_df.filter(col("prediction").isin(clusterIDoutlier:_*)).orderBy("prediction")

        saveCSV(anomalies, getAnomaliesPath(m._1, m._2))
        anomalies.write.save(getAnomaliesPath(m._1, m._2))
    }
  }

  def computeKMeans(df: DataFrame,
                        numIterations: Int = 20,
                        kVals: Seq[Int] = Seq(2, 4, 6, 8, 10, 12, 15, 20, 30, 45)): Seq[(String, Int, KMeansModel)] ={

    val Array(train, test) = df.randomSplit(Array(0.7, 0.3))

    val models = kVals.map { k =>
      val model =
        if(File(getBaseModelPath("kmeans", k)).isDirectory){
          KMeansModel.read.load(getBaseModelPath("kmeans", k))
        }
        else {
          new KMeans()
            .setK(k)
            .setMaxIter(numIterations)
            .fit(train)
        }

      val cost = model.computeCost(test)
      println(f"WCSS for K=$k id $cost%2.2f")

      model.write.overwrite.save(getBaseModelPath("kmeans", k))

      (k, model, cost)
    }

    // visualization of the cost for elbow-rules determination of k
    visualize(models.map(_._1.toDouble), models.map(_._3))

    models.map(t => ("kmeans", t._1, t._2))
  }

  def computeGMM(df: DataFrame,
                 numIterations: Int = 20,
                 kVals: Seq[Int] = Seq(2, 4, 6, 8, 10, 12, 15, 20, 30, 40)): Seq[(String, Int, GaussianMixtureModel)] ={

    val models = kVals.map { k =>
      val model =
        if(File(getBaseModelPath("gmm", k)).isDirectory){
          GaussianMixtureModel.read.load(getBaseModelPath("gmm", k))
        }
        else {
          new GaussianMixture()
            .setK(k)
            .setMaxIter(numIterations)
            .fit(df)
        }

      model.write.overwrite.save(getBaseModelPath("gmm", k))
      ("gmm", k, model)
    }

    models
  }

  def computeClusters(df: DataFrame, spark: SparkSession, algo: String, kVals: Seq[Int] = Seq()): Seq[(String, Int, Transformer)] = {
    algo match {
      case "kmeans" => if(kVals.isEmpty) computeKMeans(df) else computeKMeans(df, kVals = kVals)
      case "gmm" => if(kVals.isEmpty) computeGMM(df) else computeGMM(df, kVals = kVals)
    }
  }

}
