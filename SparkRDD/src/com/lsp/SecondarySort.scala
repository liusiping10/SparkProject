package com.lsp

import org.apache.spark.{SparkContext, SparkConf}

/**
 * * 二次排序
 * 第一步：按照ordered和Serializable接口实现自定义排序的key
 * 第二步：将要进行二次排序的文件加载进来生成key,value类型的RDD
 * 第三步：基于自定义的使用sortByKey进行二次排序
 * 第四步：去除掉排序的key，只保留排序的结果
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
 * 自定义二次排序
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
