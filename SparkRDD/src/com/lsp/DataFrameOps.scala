package com.lsp

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by liusp on 2015/12/20.
 */
object DataFrameOps {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("Spark DataFrame")
    conf.setMaster("local")
    //conf.setMaster("spark://master:7077")
    val sc = new SparkContext(conf)
    val sqlContext= new SQLContext(sc)
    val df=sqlContext.read.json("D:\\spark-1.6.1-bin-hadoop2.6\\examples\\src\\main\\resources\\people.json")

    // select * from table
    df.show()

    // desc table
    df.printSchema()

    // select name from table
    df.select("name").show()

    // select name,age+1 from table
    df.select(df.col("name"),df.col("age").plus(10)).show()

    // select * from table where age>10
    df.filter(df.col("age").gt(10)).show()

    df.groupBy(df.col("age")).count().show()

    //sc.stop()
  }
}
