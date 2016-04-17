package com.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf}

/**
 * ��״̬��WordCount
 * Created by liusp on 2016/4/16.
 */
object HdfsWordCount {
  def main(args: Array[String]) {
    val conf=new SparkConf().setAppName("HdfsWordCount").setMaster("local[2]")
    val ssc=new StreamingContext(conf,Seconds(20))
    // dataĿ¼��Ϊ�����ļ���
    val lines=ssc.textFileStream("d:\\data\\")
    val words=lines.flatMap(_.split(" "))
    val wordCounts=words.map(x=>(x,1)).reduceByKey(_+_)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
