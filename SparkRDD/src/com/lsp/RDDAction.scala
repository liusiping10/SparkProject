package com.lsp

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liusp on 2016/3/27.
  */
object RDDAction {
   def main(args: Array[String]) {
     val sc=sparkContext("test")

     reduceAction(sc)

     val numbers=sc.parallelize(1 to 100)

     val result=numbers.map(2*_)
     //result.collect()    Array[Int]
     result.collect().foreach(println)

     println(numbers.count)

     //println(numbers.take(5))

    //     numbers.countByValue()

     val scores=Array(Tuple2(1,100),Tuple2(2,90),Tuple2(1,80))

     val scoresRDD=sc.parallelize(scores)

     println(scoresRDD.countByKey())//Map(1 -> 2, 2 -> 1)

     //scoresRDD.saveAsTextFile("/data/")// PairRDD  HadoopRDD

     sc.stop

   }

   def sparkContext(name:String)={
     val conf=new SparkConf().setAppName(name).setMaster("local")
     val sc=new SparkContext(conf)
     sc
   }

   def reduceAction(sc:SparkContext): Unit ={
     val numbers=sc.parallelize(1 to 100)
     val reduces=numbers.reduce(_+_)

     println(reduces) // Int
   }
 }
