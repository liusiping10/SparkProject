package com.lsp

import org.apache.spark.{SparkConf, SparkContext}

/**
 * ���ص���WordCount����
 */
object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setAppName("WordCount")
    conf.setMaster("local")// local�Ǳ�������

    // ��ʼ�� DAGScheduler,TaskScheduler,SchedulerBackend
    val sc = new SparkContext(conf)
    //D:\spark-1.5.2-bin-hadoop2.6\README.md
    // �õ�RDD[String]  ������������
    val lines = sc.textFile("d://data.txt", 1) ///minPartitions

    println(lines.count)
    //   HadoopRDD  --��map��->MapPartitionsRDD

    //���ݿո��в��Ϊ����
    val words = lines.flatMap { line => line.split(" ") }

    // ��ÿ�����ʼ���Ϊ1
    val pairs = words.map { word => (word, 1) }

    // ͳ��ÿ�����ʵ��ܴ���
    //����ͬ��key������value�ۼӣ�����local��reduce����ͬʱreduce��
    // ����Ϊkey ����Ϊvalue
    val wordCount = pairs.reduceByKey(_ + _)

    //val wordCountOrdered=wordCount.map(pair => (pair._2,pair._1)).sortByKey(false).map(pair=>(pair._2,pair._1))
    val wordCountOrdered=wordCount.sortByKey()

    // collect�ռ�����
    wordCountOrdered.collect.foreach(wordNumberPair => println(wordNumberPair._1 + " " + wordNumberPair._2))

    // wordCount.foreach(wordNumberPair => println(wordNumberPair._1 + " " + wordNumberPair._2))

    sc.stop

  }
}
