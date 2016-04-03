package com.lsp

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameTest {
  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setAppName("WordCount")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    //D:\spark-1.5.2-bin-hadoop2.6\README.md
    val lines = sc.textFile("d://data.txt", 1)
    ///   hdfs��block��Ӧ��Ƭpartition(�����ϸ��block��С)

    val sqlContext=new SQLContext(sc)

    //DataFrame df=sqlContext.read.json("")

   // lines.repartition(2) // ���·���


    sc.stop

  }
}
