package com.streaming

import java.io.PrintWriter
import java.net.ServerSocket

import scala.io.Source

/**
 * Created by liusp on 2016/4/16.
 */
object SalesSimulation {
  def index(length:Int)={
    import java.util.Random
    val rdm=new Random()
    rdm.nextInt(length)
  }

  def main (args: Array[String]) {
    if(args.length!=3){
      System.err.println("Usage: <filename> <port> <milliseconds")
      System.exit(1)
    }

    val filename=args(0)
    val lines=Source.fromFile(filename).getLines().toList
    val fileRow=lines.length

    val listener=new ServerSocket(args(1).toInt)
    while (true){
      val socket=listener.accept()
      new Thread(){
        override def run()={
          println("Go client connected from :"+socket.getInetAddress)
          val out=new PrintWriter(socket.getOutputStream(),true)
          while(true){
            Thread.sleep(args(2).toLong)
            val content=lines(index(fileRow)) // 随机取出一行内容
            println(content)
            out.write(content+"\n")
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }
}
