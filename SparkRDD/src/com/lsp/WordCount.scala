package com.lsp

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 本地调试WordCount程序
 */
object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setAppName("WordCount")
    conf.setMaster("local")// local是本地运行

    // 初始化 DAGScheduler,TaskScheduler,SchedulerBackend
    val sc = new SparkContext(conf)
    //D:\spark-1.5.2-bin-hadoop2.6\README.md
    // 得到RDD[String]  基于行作处理
    val lines = sc.textFile("d://data.txt", 1) ///minPartitions

    println(lines.count)
    //   HadoopRDD  --（map）->MapPartitionsRDD

    //根据空格将行拆分为单词
    val words = lines.flatMap { line => line.split(" ") }

    // 将每个单词计数为1
    val pairs = words.map { word => (word, 1) }

    // 统计每个单词的总次数
    //对相同的key，进行value累加（包括local和reduce级别同时reduce）
    // 单词为key 次数为value
    val wordCount = pairs.reduceByKey(_ + _)

    //val wordCountOrdered=wordCount.map(pair => (pair._2,pair._1)).sortByKey(false).map(pair=>(pair._2,pair._1))
    val wordCountOrdered=wordCount.sortByKey()

    // collect收集数据
    wordCountOrdered.collect.foreach(wordNumberPair => println(wordNumberPair._1 + " " + wordNumberPair._2))

    // wordCount.foreach(wordNumberPair => println(wordNumberPair._1 + " " + wordNumberPair._2))

    sc.stop

  }
}
