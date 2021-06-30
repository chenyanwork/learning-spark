package com.dt.learn.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Scala开发集群运行的Spark 在线黑名单过滤程序
  *
  * @author DT大数据梦工厂
  * 新浪微博：http://weibo.com/ilovepains/
  *
  * 背景描述：在广告点击计费系统中，我们在线过滤掉黑名单的点击，进而保护广告商的利益，只进行有效的广告点击计费
  * 	或者在防刷评分（或者流量）系统，过滤掉无效的投票或者评分或者流量；
  * 实现技术：使用transform Api直接基于RDD编程，进行join操作
  *
  */
object OnlineForeachRDD2DB {
  def main(args: Array[String]){
    /**
      * 第1步：创建Spark的配置对象SparkConf，设置Spark程序的运行时的配置信息，
      * 例如说通过setMaster来设置程序要链接的Spark集群的Master的URL,如果设置
      * 为local，则代表Spark程序在本地运行，特别适合于机器配置条件非常差（例如
      * 只有1G的内存）的初学者       *
      */
    val conf = new SparkConf() //创建SparkConf对象
    conf.setAppName("OnlineForeachRDD") //设置应用程序的名称，在程序运行的监控界面可以看到名称
//    conf.setMaster("spark://Master:7077") //此时，程序在Spark集群
    conf.setMaster("local[6]")
    //设置batchDuration时间间隔来控制Job生成的频率并且创建Spark Streaming执行的入口
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.socketTextStream("Master", 9999)

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.foreachRDD { rdd =>
      rdd.partitions.isEmpty
        if (!rdd.isEmpty()){
        rdd.foreachPartition { partitionOfRecords => {
          // ConnectionPool is a static, lazily initialized pool of connections
          val connection = ConnectionPool.getConnection()
          partitionOfRecords.foreach(record => {
            val sql = "insert into streaming_itemcount(item,count) values('" + record._1 + "'," + record._2 + ")"
            val stmt = connection.createStatement();
            stmt.executeUpdate(sql);

          })
          ConnectionPool.returnConnection(connection)  // return to the pool for future reuse

        }

        }
      }

    }


    /**
      * 在StreamingContext调用start方法的内部其实是会启动JobScheduler的Start方法，进行消息循环，在JobScheduler
      * 的start内部会构造JobGenerator和ReceiverTacker，并且调用JobGenerator和ReceiverTacker的start方法：
      *   1，JobGenerator启动后会不断的根据batchDuration生成一个个的Job
      *   2，ReceiverTracker启动后首先在Spark Cluster中启动Receiver（其实是在Executor中先启动ReceiverSupervisor），在Receiver收到
      *   数据后会通过ReceiverSupervisor存储到Executor并且把数据的Metadata信息发送给Driver中的ReceiverTracker，在ReceiverTracker
      *   内部会通过ReceivedBlockTracker来管理接受到的元数据信息
      * 每个BatchInterval会产生一个具体的Job，其实这里的Job不是Spark Core中所指的Job，它只是基于DStreamGraph而生成的RDD
      * 的DAG而已，从Java角度讲，相当于Runnable接口实例，此时要想运行Job需要提交给JobScheduler，在JobScheduler中通过线程池的方式找到一个
      * 单独的线程来提交Job到集群运行（其实是在线程中基于RDD的Action触发真正的作业的运行），为什么使用线程池呢？
      *   1，作业不断生成，所以为了提升效率，我们需要线程池；这和在Executor中通过线程池执行Task有异曲同工之妙；
      *   2，有可能设置了Job的FAIR公平调度的方式，这个时候也需要多线程的支持；
      *
      */
    ssc.start()
    ssc.awaitTermination()

  }
}