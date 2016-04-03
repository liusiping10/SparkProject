package com.lsp

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setAppName("WordCount")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    //D:\spark-1.5.2-bin-hadoop2.6\README.md
    val lines = sc.textFile("d://data.txt", 1) ///
    println(lines.count)
    //   HadoopRDD  --£¨map£©->MapPartitionsRDD

    val words = lines.flatMap { line => line.split(" ") }

    val pairs = words.map { word => (word, 1) }

    val wordCount = pairs.reduceByKey(_ + _)

    //val wordCountOrdered=wordCount.map(pair => (pair._2,pair._1)).sortByKey(false).map(pair=>(pair._2,pair._1))
    val wordCountOrdered=wordCount.sortByKey()

    wordCountOrdered.collect.foreach(wordNumberPair => println(wordNumberPair._1 + " " + wordNumberPair._2))

    // wordCount.foreach(wordNumberPair => println(wordNumberPair._1 + " " + wordNumberPair._2))

    sc.stop

  }
}
