package com.dt.learn.cores

import org.apache.spark.{SparkContext, SparkConf}

/**
  * 使用Scala开发集群运行的Spark WordCount程序
  * @author DT大数据梦工厂
  * 新浪微博：http://weibo.com/ilovepains/
  * Created by DT大数据梦工厂 on 2016/1/18.
  */
object RDDBasedOnCollections {
  def main (args: Array[String]){

    val conf = new SparkConf() //创建SparkConf对象
    conf.setAppName("RDDBasedOnCollections") //设置应用程序的名称，在程序运行的监控界面可以看到名称

    conf.setMaster("local")

    val sc = new SparkContext(conf) //创建SparkContext对象，通过传入SparkConf实例来定制Spark运行的具体参数和配置信息

    val numbers = 1 to 100 //创建一个Scala集合
    val rdd = sc.parallelize(numbers)

    val sum = rdd.reduce(_+_) //1+2=3 3+3= 6 6+4=10 .....

    println("1+2+....+ 99+100 = " + sum)

  }

}
