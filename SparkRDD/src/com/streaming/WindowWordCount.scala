package com.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by liusp on 2016/4/16.
 */
object WindowWordCount {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("WindowWordCount").setMaster("local[2]")
    val sc=new SparkContext(conf)
    val ssc=new StreamingContext(sc,Seconds(5))
    ssc.checkpoint("d://data")
    // ����socket����Ϊ����
    val lines=ssc.socketTextStream(args(0),args(1).toInt,StorageLevel.MEMORY_AND_DISK_SER)
    val words=lines.flatMap(_.split(","))

    // window����
    //val wordCounts=words.map(x=>(x,1)).reduceByKeyAndWindow( (a:Int,b:Int)=>(a+b),Seconds(args(2).toInt),Seconds(args(3).toInt))
    val wordCounts=words.map(x=>(x,1)).reduceByKeyAndWindow( _+_,_-_,Seconds(args(2).toInt),Seconds(args(3).toInt))

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
