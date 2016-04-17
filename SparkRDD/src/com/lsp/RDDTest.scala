package com.lsp

import org.apache.spark.{SparkConf, SparkContext}

object RDDTest {
  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setAppName("WordCount")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val pairs = sc.parallelize(List((1, 1), (2, 2), (3, 3)))

    val a = sc.parallelize(List(2,51,2))
    val b = sc.parallelize(List(3,1,4))
    val c = a.zip(b)

    println(c.foreach(println))

    val result = pairs.join(c)
    println(result.foreach(println))
    println(result.partitioner)//Some(org.apache.spark.HashPartitioner@1)
  }
}
