package com.lsp

import org.apache.spark.{SparkContext, SparkConf}

/**
 * ��������
 * key value
 * ����key���飬value�Ӵ�С����
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

    // pair._2����ȡ��ǰ5��
    // key����
    val top5=groupRDD.map(pair=>(pair._1,pair._2.toList.sortWith(_>_).take(5))).sortByKey()

    top5.collect().foreach(pair=>{
      println("Group Key:"+pair._1)
      pair._2.foreach(println)
      println("****************")
    })

    sc.stop()
  }
}
