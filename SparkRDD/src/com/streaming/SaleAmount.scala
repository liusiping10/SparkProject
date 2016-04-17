package com.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by liusp on 2016/4/16.
 */
object SaleAmount {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("SaleAmount").setMaster("local[2]")
    val sc=new SparkContext(conf)
    val ssc=new StreamingContext(sc,Seconds(5))

    val lines=ssc.socketTextStream(args(0),args(1).toInt,StorageLevel.MEMORY_AND_DISK_SER)

    val words=lines.map(_.split(",")).filter(_.length==6)
    val wordCounts=words.map(x=>(1,x(5).toDouble)).reduceByKey(_+_)

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
