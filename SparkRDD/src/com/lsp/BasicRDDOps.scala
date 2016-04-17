package com.lsp

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by liusp on 2016/4/17.
 */
object BasicRDDOps {
  def main(args: Array[String]) {
    val conf=new SparkConf
    conf.setAppName("BasicRDDOps")
    conf.setMaster("local")

    val sc=new SparkContext(conf)

    val rdd1=sc.parallelize(1 to 10,1)

  }
}
