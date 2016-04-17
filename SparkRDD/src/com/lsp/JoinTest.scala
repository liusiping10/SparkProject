package com.lsp

import org.apache.spark.{SparkContext, SparkConf}

/**
 * 本地调试WordCount程序
 */
object JoinTest {
  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setAppName("WordCount")
    conf.setMaster("local") // local是本地运行

    val sc = new SparkContext(conf)

    val linesA = sc.textFile("D:\\spark-1.6.1-bin-hadoop2.6\\testData\\a1.txt")

    val mappedA = linesA.map {
        line =>
          val index = line.indexOf(" ")
          val (tu1,tup2)=line.splitAt(index)
          (tu1,tup2)
    }

    val linesB = sc.textFile("D:\\spark-1.6.1-bin-hadoop2.6\\testData\\b1.txt")

    val mappedB = linesB.map {
      line =>
        val index = line.indexOf(" ")
        val (tu1,tup2)=line.splitAt(index)
        (tu1,tup2)
    }

    val joined=mappedA.join(mappedB)

    println(joined.foreach(println))

    val trimed=joined.map { tu =>
      val keys = tu._2._1
      val adds = tu._2._2
      (keys.trim,adds)
    }
    trimed.foreach(println)

    trimed.map(tup=>
      tup._1+tup._2
    )
  }
}
