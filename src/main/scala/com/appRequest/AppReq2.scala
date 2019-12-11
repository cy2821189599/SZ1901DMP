package com.appRequest

import com.util.{RedisUtil, ReqUtils}
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

/**
 * 媒体指标
 */
object AppReq2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("location").master("local").getOrCreate()
    // 获取路径
    val Array(inputPath, outpuPath, app_dir) = args
    // 获取数据
    val df = spark.read.parquet(inputPath)
    // 读取字段文件
    val lines = spark.sparkContext.textFile(app_dir)
    val appMap = lines
      .filter(_.split("\t", -1).length >= 5)
      .map(_.split("\t", -1)).map(arr => (arr(4), arr(1))).collectAsMap()
    appMap.foreach(map => {
      var jedis: Jedis = null
      try {
        jedis = RedisUtil.getConnection()
        jedis.set(map._1, map._2)
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        jedis.close()
      }

    })
    df.rdd.map(row => {
      val appid = row.getAs[String]("appid")
      var appname = row.getAs[String]("appname")
      // 判断当前APPName是否为空
      if (appname.isEmpty) {
        var jedis: Jedis = null
        try {
          jedis = RedisUtil.getConnection()
          appname = jedis.get(appid)
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          jedis.close()
        }
      }
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // 业务处理方法
      val reqList = ReqUtils.reqAd(requestmode, processnode, iseffective,
        isbilling, isbid, iswin, adorderid, winprice, adpayment)
      (appname, reqList)
    }).reduceByKey((list1, list2) => {
      // list1(1,2,3,4) list2(1,2,3,4) zip(List((1,1),(2,2),(3,3),(4,4)))
      list1.zip(list2)
        // List((1+1),(2+2),(3+3),(4+4))
        .map(t => t._1 + t._2)
      // List(2,4,6,8)
    }).map(t => t._1 + " : " + t._2.mkString("<", ",", ">")).foreach(println)
  }
}
