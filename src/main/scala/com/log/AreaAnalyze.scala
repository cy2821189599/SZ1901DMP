package com.log

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object AreaAnalyze {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .appName("areaAnalyze")
      .master("local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val dataFrame = spark.read.parquet("D:\\temp\\parquet")
    dataFrame.createGlobalTempView("log")
    //地域分布
    spark.sql(
      """
        |select
        |provincename,
        |cityname,
        |originalRequest,
        |effectRequest,
        |adRequest,
        |bidCount,
        |winCount,
        |nvl(winCount/bidCount,0.0) btRate,
        |viewCount,
        |clickCount,
        |nvl(clickCount/viewCount,0.0) clickRate,
        |consume,
        |cost
        |from(
        |select
        |provincename,
        |cityname,
        |sum(case when requestmode = 1 and processnode>=1 then 1 else 0 end) originalRequest,
        |sum(case when requestmode = 1 and processnode>=2 then 1 else 0 end) effectRequest,
        |sum(case when requestmode = 1 and processnode=3 then 1 else 0 end) adRequest,
        |sum(case when isbid=1 and isbilling=1 and iseffective=1 then 1 else 0 end) bidCount,
        |sum(case when iswin=1 and isbilling=1 and iseffective=1 and adorderid!=0 then 1 else 0 end) winCount,
        |sum(case when requestmode = 2 and iseffective=1 then 1 else 0 end) viewCount,
        |sum(case when requestmode = 3 and iseffective=1 then 1 else 0 end) clickCount,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then winprice/1000 else 0 end) consume,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) cost
        |from global_temp.log
        |group by provincename,cityname
        |) tmp
        |""".stripMargin).show()
    //终端设备-运营
    spark.sql(
      """
        |select
        |ispname,
        |effectRequest,
        |adRequest,
        |bidCount,
        |winCount,
        |nvl(winCount/bidCount,0.0) btRate,
        |viewCount,
        |clickCount,
        |nvl(clickCount/viewCount,0.0) clickRate,
        |consume,
        |cost
        |from(
        |select
        |ispname,
        |count(*) totalReq,
        |sum(case when requestmode = 1 and processnode>=2 then 1 else 0 end) effectRequest,
        |sum(case when requestmode = 1 and processnode=3 then 1 else 0 end) adRequest,
        |sum(case when isbid=1 and isbilling=1 and iseffective=1 then 1 else 0 end) bidCount,
        |sum(case when iswin=1 and isbilling=1 and iseffective=1 and adorderid!=0 then 1 else 0 end) winCount,
        |sum(case when requestmode = 2 and iseffective=1 then 1 else 0 end) viewCount,
        |sum(case when requestmode = 3 and iseffective=1 then 1 else 0 end) clickCount,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then winprice/1000 else 0 end) consume,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) cost
        |from global_temp.log
        |group by ispname
        |)tmp
        |""".stripMargin).show()

    //终端设备-网络类
    spark.sql(
      """
        |select
        |networkmannername,
        |effectRequest,
        |adRequest,
        |bidCount,
        |winCount,
        |nvl(winCount/bidCount,0.0) btRate,
        |viewCount,
        |clickCount,
        |nvl(clickCount/viewCount,0.0) clickRate,
        |consume,
        |cost
        |from(
        |select
        |networkmannername,
        |count(*) totalReq,
        |sum(case when requestmode = 1 and processnode>=2 then 1 else 0 end) effectRequest,
        |sum(case when requestmode = 1 and processnode=3 then 1 else 0 end) adRequest,
        |sum(case when isbid=1 and isbilling=1 and iseffective=1 then 1 else 0 end) bidCount,
        |sum(case when iswin=1 and isbilling=1 and iseffective=1 and adorderid!=0 then 1 else 0 end) winCount,
        |sum(case when requestmode = 2 and iseffective=1 then 1 else 0 end) viewCount,
        |sum(case when requestmode = 3 and iseffective=1 then 1 else 0 end) clickCount,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then winprice/1000 else 0 end) consume,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) cost
        |from global_temp.log
        |group by networkmannername
        |)tmp
        |""".stripMargin).show()
    //终端设备-操作系统
    spark.sql(
      """
        |select
        |case when client=1 then "android" when client=2 then "ios" when client=3 then "wp" else "其他" end os,
        |effectRequest,
        |adRequest,
        |bidCount,
        |winCount,
        |nvl(winCount/bidCount,0.0) btRate,
        |viewCount,
        |clickCount,
        |nvl(clickCount/viewCount,0.0) clickRate,
        |consume,
        |cost
        |from(
        |select
        |client,
        |count(*) totalReq,
        |sum(case when requestmode = 1 and processnode>=2 then 1 else 0 end) effectRequest,
        |sum(case when requestmode = 1 and processnode=3 then 1 else 0 end) adRequest,
        |sum(case when isbid=1 and isbilling=1 and iseffective=1 then 1 else 0 end) bidCount,
        |sum(case when iswin=1 and isbilling=1 and iseffective=1 and adorderid!=0 then 1 else 0 end) winCount,
        |sum(case when requestmode = 2 and iseffective=1 then 1 else 0 end) viewCount,
        |sum(case when requestmode = 3 and iseffective=1 then 1 else 0 end) clickCount,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then winprice/1000 else 0 end) consume,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) cost
        |from global_temp.log
        |group by client
        |)tmp
        |""".stripMargin).show()

    // 媒体分析
    val mediaDF = spark.sql(
      """
        |select
        |appname,
        |effectRequest,
        |adRequest,
        |bidCount,
        |winCount,
        |nvl(winCount/bidCount,0.0) btRate,
        |viewCount,
        |clickCount,
        |nvl(clickCount/viewCount,0.0) clickRate,
        |consume,
        |cost
        |from(
        |select
        |appname,
        |count(*) totalReq,
        |sum(case when requestmode = 1 and processnode>=2 then 1 else 0 end) effectRequest,
        |sum(case when requestmode = 1 and processnode=3 then 1 else 0 end) adRequest,
        |sum(case when isbid=1 and isbilling=1 and iseffective=1 then 1 else 0 end) bidCount,
        |sum(case when iswin=1 and isbilling=1 and iseffective=1 and adorderid!=0 then 1 else 0 end) winCount,
        |sum(case when requestmode = 2 and iseffective=1 then 1 else 0 end) viewCount,
        |sum(case when requestmode = 3 and iseffective=1 then 1 else 0 end) clickCount,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then winprice/1000 else 0 end) consume,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) cost
        |from global_temp.log
        |group by appname
        |)tmp
        |""".stripMargin)
    mediaDF.sample(false, 0.3).show()
    //渠道表表
    val platformDF = spark.sql(
      """
        |select
        |adplatformproviderid,
        |effectRequest,
        |adRequest,
        |bidCount,
        |winCount,
        |nvl(winCount/bidCount,0.0) btRate,
        |viewCount,
        |clickCount,
        |nvl(clickCount/viewCount,0.0) clickRate,
        |consume,
        |cost
        |from(
        |select
        |adplatformproviderid,
        |count(*) totalReq,
        |sum(case when requestmode = 1 and processnode>=2 then 1 else 0 end) effectRequest,
        |sum(case when requestmode = 1 and processnode=3 then 1 else 0 end) adRequest,
        |sum(case when isbid=1 and isbilling=1 and iseffective=1 then 1 else 0 end) bidCount,
        |sum(case when iswin=1 and isbilling=1 and iseffective=1 and adorderid!=0 then 1 else 0 end) winCount,
        |sum(case when requestmode = 2 and iseffective=1 then 1 else 0 end) viewCount,
        |sum(case when requestmode = 3 and iseffective=1 then 1 else 0 end) clickCount,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then winprice/1000 else 0 end) consume,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) cost
        |from global_temp.log
        |group by adplatformproviderid
        |)tmp
        |""".stripMargin)
    platformDF.sample(false, 0.3).show()
    spark.stop()
  }

}
