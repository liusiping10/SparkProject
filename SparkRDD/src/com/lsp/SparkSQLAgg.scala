package com.lsp

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * SparkSQL���ú���
 * ʹ��SparkSQL���ú����������ݷ���
 * DataFrame�е����ú��������Ľ���Ƿ���һ��column����
 * DataFrame: A distributed collection of data organized into named columns
 * ���и��ӵ����ݷ���������ʵ��ģ�͵�ӳ��
 * Spark1.5֮���ṩ�˴��˵����ú�������agg    [�ص�] aggregation�ۺϲ���
 *
 * def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame = {
    groupBy().agg(aggExpr, aggExprs : _*)
  }
 * def agg(expr: Column, exprs: Column*): DataFrame = {
    toDF((expr +: exprs).map(_.expr))
  }

 * max,mean,min,sum,avg,explode,size,sort_array,day,to_date,abs,acros.asin,atan
 * 5���������
 * 1.�ۺϺ��� countDistinct,sumDistinct
 * 2.���Ϻ��� sort_array,explode
 * 3.���ڣ�ʱ�亯�� hour,quarter,next_day
 * 4.��ѧ���� asin,atan,sqrt,tan,round
 * 5.�������� rowNumber
 * 6.�ַ������� concat,format_number,rexexp_extract
 * 7.�������� isNaN,sha,randn,callUDF,callUDAF
 */
object SparkSQLAgg {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("SQLAgg").setMaster("local")

    val sc=new SparkContext(conf)

    // HiveContext�̳�SQLContext�����⻹��������ĺ������� import org.apache.spark.sql.hive.HiveContext
    // ����SQL������
    val sqlContext=new SQLContext(sc)

    //Ҫʹ��SparkSQL�����ú�����һ��Ҫ����HiveContext�µ���ʽת��
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

    // �����ֲ�ʽ���϶���
    val userDataRDD=sc.parallelize(userData)

    // ����ҵ����Ҫ�������ݽ���Ԥ��������DataFrame��Ҫ���RDDת����DataFrame���ͣ���Ҫ�Ȱ�RDD
    //�е�Ԫ�����ͱ��Row���ͣ�ͬʱҪ�ṩDataFrame�е�columns��Ԫ������Ϣ����
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

    // ͨ��sqlContext������RDDRow�ͽṹ��Ϣ������DataFrame
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

    // ʹ��SparkSQL���ܵ����ú�����DataFrame���в���
    // �ر�ע�⣬���ú������ɵ�column�������Զ�����CG
   userDataDF.groupBy("time").agg('time,countDistinct('id)).show()
    /*
+---------+---------+------------------+
|     time|     time|COUNT(DISTINCT id)|
+---------+---------+------------------+
|2016-3-27|2016-3-27|                 2|
|2016-3-28|2016-3-28|                 4|
+---------+---------+------------------+
     */

    // һ�������վ���û���ͳ��UV
    userDataDF.groupBy("time").agg('time,countDistinct('id))
    .map(row=>Row(row(1),row(2))).collect().foreach(println)
    /*
    [2016-3-27,2]
    [2016-3-28,4]
     */

    // ͳ��ÿ���amount��
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
