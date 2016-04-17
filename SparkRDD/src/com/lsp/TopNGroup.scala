package com.lsp

import org.apache.spark.{SparkContext, SparkConf}

/**
 * 分组排序
 * key value
 * 根据key分组，value从大到小排序
 * Created by liusp on 2016/4/18.
 */
object TopNGroup {
  def main(args: Array[String]) {
    val conf=new SparkConf
    conf.setAppName("TopNGroup")
    conf.setMaster("local")

    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val lines=sc.textFile("d:\\data\\TopNGroup.txt")

    val groupRDD=lines.map { line =>
      val splited = line.split(" ")
      (splited(0),splited(1).toInt)
    }.groupByKey()

    // pair._2排序，取出前5个
    // key排序
    val top5=groupRDD.map(pair=>(pair._1,pair._2.toList.sortWith(_>_).take(5))).sortByKey()

    top5.collect().foreach(pair=>{
      println("Group Key:"+pair._1)
      pair._2.foreach(println)
      println("****************")
    })

    sc.stop()
  }
}
