package com.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by liusp on 2016/4/16.
 */
object StatefulWordCount {
  def main(args: Array[String]) {
    val updateFunc=(values:Seq[Int],state:Option[Int])=>{
      val currentCount=values.foldLeft(0)(_+_)
      val previousCount=state.getOrElse(0)
      Some(currentCount+previousCount)
    }

    val conf=new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]")
    val sc=new SparkContext(conf)
    val ssc=new StreamingContext(sc,Seconds(5))
    ssc.checkpoint("d://data")
    // 网络socket流作为输入
    val lines=ssc.socketTextStream(args(0),args(1).toInt)
    val words=lines.flatMap(_.split(","))
    val wordCounts=words.map(x=>(x,1))

    // 使用updateStateByKey来更新状态
    val stateStream=wordCounts.updateStateByKey[Int](updateFunc)
    stateStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
