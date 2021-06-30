package com.dt.learn.cores

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用Scala开发集群运行的Spark WordCount程序
  * @author DT大数据梦工厂
  * 新浪微博：http://weibo.com/ilovepains/
  * Created by DT大数据梦工厂 on 2016/1/18.
  */
object RDDBasedOnLocalFile {
  def main (args: Array[String]){

    val conf = new SparkConf() //创建SparkConf对象
    conf.setAppName("RDDBasedOnLocalFile") //设置应用程序的名称，在程序运行的监控界面可以看到名称

    conf.setMaster("local")

    val sc = new SparkContext(conf) //创建SparkContext对象，通过传入SparkConf实例来定制Spark运行的具体参数和配置信息

    val rdd = sc.textFile("D://Big_Data_Software//spark-1.6.0-bin-hadoop2.6//README.md")

    val linesLength = rdd.map(line => line.length)

    val sum = linesLength.reduce(_+_)

    println("The total characters of the file is : " + sum)

  }

}
