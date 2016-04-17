package com.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 网络流
 * Created by liusp on 2016/4/16.
 */
object NetworkWordCount {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val sc=new SparkContext(conf)
    val ssc=new StreamingContext(sc,Seconds(5))
    // 网络socket流作为输入
    val lines=ssc.socketTextStream(args(0),args(1).toInt,StorageLevel.MEMORY_AND_DISK_SER)
    val words=lines.flatMap(_.split(","))
    val wordCounts=words.map(x=>(x,1)).reduceByKey(_+_)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
