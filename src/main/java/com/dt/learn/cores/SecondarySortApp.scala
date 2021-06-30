package com.dt.learn.cores

import org.apache.spark.{SparkContext, SparkConf}

/**
  * 二次排序，具体的实现步骤：
  * 第一步：按照Ordered和Serrializable接口实现自定义排序的Key
  * 第二步：将要进行二次排序的文件加载进来身材<Key,Value>类型的RDD
  * 第三步：使用sortByKey基于自定义的Key进行二次排序
  * 第四步：去除掉排序的Key，只保留排序的结果
  * @author DT大数据梦工厂
  * 新浪微博：http://weibo.com/ilovepains/
  */
object SecondarySortApp {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SecondarySortApp").setMaster("local") //创建SparkConf，初始化程序的配置
    val sc = new SparkContext(conf) //创建SparkContext，这是第一个RDD创建的唯一入口，也是Driver的灵魂，是通往集群的唯一通道

    val lines = sc.textFile("D://Big_Data_Software//spark-1.6.0-bin-hadoop2.6//helloSpark.txt")

    val  pairWithSortKey = lines.map(line => (
      new SecondarySortKey(line.split(" ")(0).toInt, line.split(" ")(1).toInt), line
      ))

    val sorted = pairWithSortKey.sortByKey(false)

    val sortedResult = sorted.map(sortedLine => sortedLine._2)

    sortedResult.collect().foreach(println)
  }

}
