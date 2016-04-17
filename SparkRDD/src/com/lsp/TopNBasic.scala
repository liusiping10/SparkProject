package com.lsp

import org.apache.spark.{SparkContext, SparkConf}

/**
 * 基本的排序
 * 从大到小
 * topN
 * Created by liusp on 2016/4/18.
 */
object TopNBasic {
  def main(args: Array[String]) {
    val conf=new SparkConf
    conf.setAppName("TopNBasic")
    conf.setMaster("local")

    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val lines=sc.textFile("d:\\data\\TopNBasic.txt")

    // 生成key-value键值对,方便sortByKey进行排序
    val pairs=lines.map(line=>(line.toInt,line))

    // 降序排列
    val sortedPairs=pairs.sortByKey(false)

    // 过滤出排序的内容
    val sortedData=sortedPairs.map(pair=>pair._2)

    // 获取top5的元素内容，构建一个Array
    val top5=sortedData.take(5)

    top5.foreach(println)

    sc.stop()
  }
}
