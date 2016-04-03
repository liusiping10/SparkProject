package com.lsp

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * SparkSQL内置函数
 * 使用SparkSQL内置函数进行数据分析
 * DataFrame中的内置函数操作的结果是返回一个column对象
 * DataFrame: A distributed collection of data organized into named columns
 * 进行复杂的数据分析，基于实际模型的映射
 * Spark1.5之后，提供了打了的内置函数，如agg    [重点] aggregation聚合操作
 *
 * def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame = {
    groupBy().agg(aggExpr, aggExprs : _*)
  }
 * def agg(expr: Column, exprs: Column*): DataFrame = {
    toDF((expr +: exprs).map(_.expr))
  }

 * max,mean,min,sum,avg,explode,size,sort_array,day,to_date,abs,acros.asin,atan
 * 5大基本类型
 * 1.聚合函数 countDistinct,sumDistinct
 * 2.集合函数 sort_array,explode
 * 3.日期，时间函数 hour,quarter,next_day
 * 4.数学函数 asin,atan,sqrt,tan,round
 * 5.开窗函数 rowNumber
 * 6.字符串函数 concat,format_number,rexexp_extract
 * 7.其他函数 isNaN,sha,randn,callUDF,callUDAF
 */
object SparkSQLAgg {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("SQLAgg").setMaster("local")

    val sc=new SparkContext(conf)

    // HiveContext继承SQLContext，另外还包含额外的函数功能 import org.apache.spark.sql.hive.HiveContext
    // 构建SQL上下文
    val sqlContext=new SQLContext(sc)

    //要使用SparkSQL的内置函数，一定要导入HiveContext下的隐式转换
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val userData = Array(
      "2016-3-27,001,http://spark.apache.org/,1000",
      "2016-3-27,001,http://hadoop.apache.org/,1001",
      "2016-3-27,002,http://flink.apache.org/,1002",
      "2016-3-28,003,http://kafka.apache.org/,1020",
      "2016-3-28,004,http://spark.apache.org/,1010",
      "2016-3-28,002,http://hive.apache.org/,1200",
      "2016-3-28,001,http://parquet.apache.org/,1500",
      "2016-3-28,001,http://spark.apache.org/,1800"
    )

    // 产生分布式集合对象
    val userDataRDD=sc.parallelize(userData)

    // 根据业务需要，对数据进行预处理，生成DataFrame，要想把RDD转换成DataFrame类型，需要先把RDD
    //中的元素类型变成Row类型，同时要提供DataFrame中的columns的元数据信息描述
    val userDataRDDRow=userDataRDD.map(
      row=>{
        val splited=row.split(",")
        Row(splited(0),splited(1).toInt,splited(2),splited(3).toInt)
      })

    val structTypes=StructType(
      Array(
        StructField("time",StringType,true),
        StructField("id",IntegerType,true),
        StructField("url",StringType,true),
        StructField("amount",IntegerType,true)
      )
    )

    // 通过sqlContext，根据RDDRow和结构信息，创建DataFrame
    val userDataDF=sqlContext.createDataFrame(userDataRDDRow,structTypes)

    println(userDataDF.show)
    /*
+---------+---+--------------------+------+
|     time| id|                 url|amount|
+---------+---+--------------------+------+
|2016-3-27|  1|http://spark.apac...|  1000|
|2016-3-27|  1|http://hadoop.apa...|  1001|
|2016-3-27|  2|http://flink.apac...|  1002|
|2016-3-28|  3|http://kafka.apac...|  1020|
|2016-3-28|  4|http://spark.apac...|  1010|
|2016-3-28|  2|http://hive.apach...|  1200|
|2016-3-28|  1|http://parquet.ap...|  1500|
|2016-3-28|  1|http://spark.apac...|  1800|
+---------+---+--------------------+------+
     */

    // 使用SparkSQL功能的内置函数对DataFrame进行操作
    // 特别注意，内置函数生成的column对象且自动进行CG
   userDataDF.groupBy("time").agg('time,countDistinct('id)).show()
    /*
+---------+---------+------------------+
|     time|     time|COUNT(DISTINCT id)|
+---------+---------+------------------+
|2016-3-27|2016-3-27|                 2|
|2016-3-28|2016-3-28|                 4|
+---------+---------+------------------+
     */

    // 一天访问网站的用户数统计UV
    userDataDF.groupBy("time").agg('time,countDistinct('id))
    .map(row=>Row(row(1),row(2))).collect().foreach(println)
    /*
    [2016-3-27,2]
    [2016-3-28,4]
     */

    // 统计每天的amount和
    userDataDF.groupBy("time").agg('time,sum('amount)).show()
    /*
    +---------+---------+-----------+
    |     time|     time|sum(amount)|
    +---------+---------+-----------+
    |2016-3-27|2016-3-27|       3003|
    |2016-3-28|2016-3-28|       6530|
    +---------+---------+-----------+
     */
  }
}
