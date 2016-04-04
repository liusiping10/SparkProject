package com.lsp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*
	提交WordCount程序到集群运行
 */
object WordCountCluster {
  // jar file
  // 提交到集群的命令
  /*	
   wordcount.sh
  	bin/spark-submit --master spark://master:7077
  				--class com.lsp.WordCountCluster
  				/home/liusp/wordcount.jar
  */
  def main(args: Array[String]) {
	  val conf=new SparkConf
	  conf.setAppName("WordCount")
	  // spark-submit --master spark://master:7077
	  //conf.setMaster("spark://master:7077") 
	  
	  // 初始化 DAGScheduler,TaskScheduler,SchedulerBackend
	  val sc=new SparkContext(conf)
	  
	  //RDD[String]  基于行作处理
	  // 自动感知上下文，读取hdfs文件
	  // 并切分成不同的partitions
	  val lines=sc.textFile("/input/data")
	  //sc.textFile("hdfs://master:9000/input/data")
	  
	  //根据空格将行拆分为单词
	  val words=lines.flatMap{line => line.split(" ")}
	  
	  // 将每个单词计数为1
	  val pairs=words.map{word => (word,1)}
	  
	  // 统计每个单词的总次数    
	  //对相同的key，进行value累加（包括local和reduce级别同时reduce）
	  // 单词为key 次数为value
	  val wordCount=pairs.reduceByKey(_+_)
	  
	  wordCount.foreach(wordNumberPair=>println(wordNumberPair._1+" "+wordNumberPair._2))
	  
	  sc.stop
  }
}