package com.lsp

import org.apache.spark.{SparkConf, SparkContext}

object RDDCollection {
  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setAppName("WordCount")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val numbers= 1 to 100

    val rdd=sc.parallelize(numbers)

    val sum=rdd.reduce(_+_)

    println(sum)

    sc.stop

  }
}
