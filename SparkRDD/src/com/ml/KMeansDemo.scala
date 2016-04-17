package com.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by liusp on 2016/4/17.
 */
object KMeansDemo {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf=new SparkConf().setAppName("KMeans").setMaster("local[4]")
    val sc=new SparkContext(conf)
    // ÉèÖÃ1¸öpartition
    val data=sc.textFile("d:\\data\\ml\\kmeans_data.txt",1)
    val parsedData=data.map(s=>Vectors.dense(s.split(" ").map(_.toDouble)))
    //WARN KMeans: The input data was not directly cached, which may hurt performance if its parent RDDs are also uncached.
    parsedData.cache()

    val numClusters=2
    val numIterations=20
    val model=KMeans.train(parsedData,numClusters,numIterations)

    println("Cluster center:")
    for(c <- model.clusterCenters){
      println(" "+c.toString)
    }

    val cost=model.computeCost(parsedData)
    println("The Sum of Squares Errors:"+cost)

    println("0.2 vector:"+model.predict(Vectors.dense("0.2 0.2 0.2".split(" ").map(_.toDouble))))
    println("0.25 vector:"+model.predict(Vectors.dense("0.25 0.25 0.25".split(" ").map(_.toDouble))))
    println("8 vector:"+model.predict(Vectors.dense("8 8 8".split(" ").map(_.toDouble))))

    val testData=data.map(s=>Vectors.dense(s.split(" ").map(_.toDouble)))
    val result1=model.predict(testData)
    result1.saveAsTextFile("d:\\data\\ml\\kmeans_result1")

    val result2=data.map{
      line=>
        val lineVector=Vectors.dense(line.split(" ").map(_.toDouble))
        val predictions=model.predict(lineVector)
        line+" "+predictions
    }
    result2.saveAsTextFile("d:\\data\\ml\\kmeans_result2")
  }
}
