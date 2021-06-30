package com.dt.learn.sparkstreaming

import io.netty.handler.codec.string.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * @author DT大数据梦工厂
 * 新浪微博：http://weibo.com/ilovepains/
 * 
 *
  */
case class MessageItem (name: String, age:Int)

object SparkStreamingFromKakfkaFlume2Hive {
    def main(args: Array[String]){

      if (args.length < 2){
        System.err.println("Please input your Kafka Broker List and topics to consume")
        System.exit(1)
      }




      val conf = new SparkConf() //创建SparkConf对象
      conf.setAppName("OnlineHottestItems") //设置应用程序的名称，在程序运行的监控界面可以看到名称
      conf.setMaster("spark://Master:7077") //此时，程序在Spark集群


      val ssc = new StreamingContext(conf, Seconds(5))


      val Array(brokers, topiclist) = args
      val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
      val topics = topiclist.split(",").toSet


//      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topics)
//        .map(_._2.split(",")).foreachRDD(rdd => {
//
//
//        val hiveContext = new org.apache.spark.sql.hive.HiveContext(rdd.sparkContext)
//
//        import hiveContext.implicits._
//        rdd.map(record => MessageItem(record(0).trim, record(1).trim.toInt)).toDF.registerTempTable("temp")
//        hiveContext.sql("select count(*) from temp").show()
//      })

      /**
        * 大家今天的作业：
        *   1，把数据写入到Hive中；
        *   2，通过Java技术访问Hive中的内容；
        *   3，通过Flume做最原始的数据收集；
        *   4，Flume会作为Kafka的Producer把数据写入到Kafka中供本程序消费处理；
        */

      ssc.start()
      ssc.awaitTermination()

    }
}