package com.log

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 将数据结果存入mysql或HDFS
  */
object par2MySQL {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    if(args.length!=2){
      sys.exit()
    }
    // 获取路径
    val Array(inputPath,outputPath) = args
    val spark = SparkSession.builder().master("local").appName("mysql").getOrCreate()
    // 读取数据
    val df = spark.read.parquet(inputPath)
    // 注册临时视图
    df.createTempView("log")
    // 执行SQL
    val result = spark.sql("select provincename,cityname,count(*) ct from log group by provincename,cityname")
    // 配置mysql属性
    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.username"))
    prop.setProperty("password",load.getString("jdbc.password"))
    // 写入mysql
    // result.write.mode(SaveMode.Append).jdbc(load.getString("jdbc.url"),"procity",prop)
    result.write.partitionBy("provincename","cityname").json(outputPath)
    spark.stop()
  }
}
