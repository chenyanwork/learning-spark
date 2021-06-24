package com.oreilly.learningsparkexamples.scala

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object BaseMysql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("MySqlRdd")
    val sc= new SparkContext(conf)
    // mysql配置信息
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://10.1.135.155:3307/test"
    val user = "dev_common"
    val password = "1qaz@WSX"
    val jdbcRDD = new JdbcRDD[String](  // 传入sc生成rdd
      sc,
      // 获取数据库连接
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, user, password)
      },
      "select job_name from app_job_test where ? <= job_id and job_id <= ?",
      // 下界
      1,
      // 上界
      10,
      // 分区数
      1,
      // 处理resultSet
      rs => rs.getString("job_name")
    )
    // 打印数据
    jdbcRDD.foreach(println)
    // 关闭连接
    sc.stop()
  }
}
