package com.lsp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/*
	�ύWordCount���򵽼�Ⱥ����
 */
object WordCountCluster {
  // jar file
  // �ύ����Ⱥ������
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
	  
	  // ��ʼ�� DAGScheduler,TaskScheduler,SchedulerBackend
	  val sc=new SparkContext(conf)
	  
	  //RDD[String]  ������������
	  // �Զ���֪�����ģ���ȡhdfs�ļ�
	  // ���зֳɲ�ͬ��partitions
	  val lines=sc.textFile("/input/data")
	  //sc.textFile("hdfs://master:9000/input/data")
	  
	  //���ݿո��в��Ϊ����
	  val words=lines.flatMap{line => line.split(" ")}
	  
	  // ��ÿ�����ʼ���Ϊ1
	  val pairs=words.map{word => (word,1)}
	  
	  // ͳ��ÿ�����ʵ��ܴ���    
	  //����ͬ��key������value�ۼӣ�����local��reduce����ͬʱreduce��
	  // ����Ϊkey ����Ϊvalue
	  val wordCount=pairs.reduceByKey(_+_)
	  
	  wordCount.foreach(wordNumberPair=>println(wordNumberPair._1+" "+wordNumberPair._2))
	  
	  sc.stop
  }
}