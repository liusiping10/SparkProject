package com.lsp

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liusp on 2016/3/27.
  */
object RDDJoin {
   def main(args: Array[String]) {
     val sc=sparkContext("test")

     joinTransformation(sc)

     sc.stop

   }

   def sparkContext(name:String)={
     val conf=new SparkConf().setAppName(name).setMaster("local")
     val sc=new SparkContext(conf)
     sc
   }

   def joinTransformation(sc:SparkContext): Unit ={
     // 学生的成绩
     val stuName=Array(
      Tuple2(1,"Spark"),
       Tuple2(2,"Tachyon"),
       Tuple2(3,"Hadoop"),
       Tuple2(5,"Java")
     )

     val stuScore=Array(
       Tuple2(1,100),
       Tuple2(2,95),
       Tuple2(3,65),
       Tuple2(4,65)
     )

     val names=sc.parallelize(stuName)
     val scores=sc.parallelize(stuScore)

     val stuNameAndScore=names.join(scores)
     stuNameAndScore.collect().foreach(println)
     //(1,(Spark,100))
     //(3,(Hadoop,65))
     //(2,(Tachyon,95))

     val stuNameAndScoreLeft=names.leftOuterJoin(scores)
     stuNameAndScoreLeft.collect().foreach(println)
     /*
     (1,(Spark,Some(100)))
     (3,(Hadoop,Some(65)))
     (5,(Java,None))
     (2,(Tachyon,Some(95)))
      */
   }
 }
