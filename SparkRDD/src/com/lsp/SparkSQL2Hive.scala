package com.lsp

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSQL2Hive {
  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setAppName("SparkSQL2Hive")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val hiveContext=new HiveContext(sc)



    sc.stop

  }
}
