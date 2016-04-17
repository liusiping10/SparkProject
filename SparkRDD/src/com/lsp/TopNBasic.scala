package com.lsp

import org.apache.spark.{SparkContext, SparkConf}

/**
 * ����������
 * �Ӵ�С
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

    // ����key-value��ֵ��,����sortByKey��������
    val pairs=lines.map(line=>(line.toInt,line))

    // ��������
    val sortedPairs=pairs.sortByKey(false)

    // ���˳����������
    val sortedData=sortedPairs.map(pair=>pair._2)

    // ��ȡtop5��Ԫ�����ݣ�����һ��Array
    val top5=sortedData.take(5)

    top5.foreach(println)

    sc.stop()
  }
}
