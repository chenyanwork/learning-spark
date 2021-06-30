package com.dt.learn.cores

import org.apache.spark.{SparkContext, SparkConf}

/**
  * 使用Scala开发集群运行的Text Lines程序
  * @author DT大数据梦工厂
  * 新浪微博：http://weibo.com/ilovepains/
  */
object TextLines {

  def main (args: Array[String]) {
    val conf = new SparkConf() //创建SparkConf对象
    conf.setAppName("Text Lines ") //设置应用程序的名称，在程序运行的监控界面可以看到名称
    //    conf.setMaster("spark://Master:7077") //此时，程序在Spark集群
    conf.setMaster("local")
    /**
      * 建SparkContext对象
      * SparkContext是Spark程序所有功能的唯一入口，无论是采用Scala、Java、Python、R等都必须有一个SparkContext
      * SparkContext核心作用：初始化Spark应用程序运行所需要的核心组件，包括DAGScheduler、TaskScheduler、SchedulerBackend
      * 同时还会负责Spark程序往Master注册程序等
      * SparkContext是整个Spark应用程序中最为至关重要的一个对象
      */
    val sc = new SparkContext(conf) //创建SparkContext对象，通过传入SparkConf实例来定制Spark运行的具体参数和配置信息

    val lines = sc.textFile("C:\\Users\\hp\\Desktop\\textLines.txt") //通过HadoopRDD以及MapPartitionsRDD获取文件中每一行的内容本身

    val lineCount= lines.map(line => (line, 1)) //每一行变成行的内容与1构成的Tuple

    val textLines = lineCount.reduceByKey(_+_)

    textLines.collect.foreach(pair => println(pair._1 + " : " +pair._2))

//    textLines.foreach(pair => println(pair._1 + " : " +pair._2))
  }

}
