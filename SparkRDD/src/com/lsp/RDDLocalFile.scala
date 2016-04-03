package com.lsp

import org.apache.spark.{SparkConf, SparkContext}

object RDDLocalFile {
  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setAppName("WordCount")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val rdd = sc.textFile("D:\\spark-1.5.2-bin-hadoop2.6\\README.md", 1)

    val linesLength=rdd.map(line=>line.length)

    val sum=linesLength.reduce(_+_)

    println(sum)

    sc.stop

  }
}
