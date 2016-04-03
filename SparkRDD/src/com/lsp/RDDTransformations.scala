package com.lsp

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by liusp on 2016/3/27.
 */
object RDDTransformations {
  def main(args: Array[String]) {
    /*val conf=new SparkConf().setAppName("transforms").setMaster("local")

    val sc=new SparkContext(conf)

    // ���ݼ��ϴ���RDD
    val nums=sc.parallelize(1 to 10)
    println(nums.count)

    val mapped=nums.map(item=>item*2)
    mapped.collect.foreach(println)

    val filterd=nums.filter(item=>item%2==0)
    filterd.collect.foreach(println)

    val bigData=Array("Scala Spark","Java Hadoop","Java Tackyon")
    val bigDataString=sc.parallelize(bigData)

    val words=bigDataString.flatMap(_.split(" "))
    words.collect().foreach(println)

    sc.stop()*/

    val sc=sparkContext("test")
    //mapTransformation(sc)
    //filterTransformation(sc)
    //flatMapTransformation(sc)

    //groupByKeyTransformation(sc)
    reduceByKeyTransformation(sc)

    sc.stop

  }

  def sparkContext(name:String)={
    val conf=new SparkConf().setAppName(name).setMaster("local")
    val sc=new SparkContext(conf)
    sc
  }

  def mapTransformation(sc:SparkContext): Unit ={
    val nums=sc.parallelize(1 to 10)

    val mapped=nums.map(item=>item*2)//�õ�MapPartitionsRDD

    mapped.collect.foreach(println)
  }

  def filterTransformation(sc:SparkContext): Unit ={
    val nums=sc.parallelize(1 to 10)
    println(nums.count)

    val mapped=nums.filter(item=>item%2==0)//�õ�MapPartitionsRDD
    mapped.collect.foreach(println)
  }

  def flatMapTransformation(sc:SparkContext): Unit ={
    val bigData=Array("Scala Spark","Java Hadoop","Java Tackyon")
    val bigDataString=sc.parallelize(bigData)// �õ�ParallelCollectionRDD

    val words=bigDataString.flatMap(_.split(" ")) // �з�֮�󣬱�ƽ������
    words.collect().foreach(println)
  }

  def groupByKeyTransformation(sc:SparkContext): Unit ={
    val data=Array(Tuple2(100,"Spark"),Tuple2(89,"Scala"),Tuple2(100,"Java"))
    val dataRDD=sc.parallelize(data)

    val grouped=dataRDD.groupByKey() // ������ͬ��key��value���з��飬������value��һ������

    grouped.collect().foreach(println)
    //(100,CompactBuffer(Spark, Java))
    //(89,CompactBuffer(Scala))
  }

  def reduceByKeyTransformation(sc:SparkContext): Unit ={
    val data=Array("Spark","Scala","Scala Scala","Hadoop")
    val dataRDD=sc.parallelize(data)

    val mapped=dataRDD.flatMap(_.split(" ")).map(word=>(word,1))  // tuple
    val reduced=mapped.reduceByKey(_+_)  //tuple

    reduced.collect().foreach(pair=>println(pair._1+" "+pair._2))

    // ����
    val sorted=reduced.sortByKey()  //tuple

    sorted.collect().foreach(pair=>println(pair._1+" "+pair._2))

  }
}
