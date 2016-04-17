package com.lsp

import org.apache.spark.{SparkContext, SparkConf}

/**
 * * ��������
 * ��һ��������ordered��Serializable�ӿ�ʵ���Զ��������key
 * �ڶ�������Ҫ���ж���������ļ����ؽ�������key,value���͵�RDD
 * �������������Զ����ʹ��sortByKey���ж�������
 * ���Ĳ���ȥ���������key��ֻ��������Ľ��
 */
object SecondarySort {
  def main(args: Array[String]) {
    val conf=new SparkConf
    conf.setAppName("TopNGroup")
    conf.setMaster("local")

    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val lines=sc.textFile("d:\\data\\helloSort.txt")

    val pairWithSortKey=lines.map(line=> {
      val splited = line.split(" ")
      (new SecondarySortKey(splited(0).toInt,splited(1).toInt),line)
    })

    val sorted=pairWithSortKey.sortByKey(false)

    val sortedResult=sorted.map(line=>line._2)
    sortedResult.collect().foreach(println)
  }
}

/**
 * �Զ����������
 */
class SecondarySortKey(val first:Int,val second:Int) extends Ordered[SecondarySortKey] with Serializable {
  def compare(other:SecondarySortKey): Int ={
    if(this.first-other.first!=0){
      this.first-other.first
    }else{
      this.second-other.second
    }
  }
}
