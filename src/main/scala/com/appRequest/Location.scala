package com.appRequest

import org.apache.spark.sql.SparkSession

/**
  * 地域指标统计 SQL
  */
object Location {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("location").master("local").getOrCreate()
    // 获取路径
    val Array(inputPath,outpuPath)=args

    // 获取数据
    val df = spark.read.parquet(inputPath)
    df.createTempView("log")
    spark.sql(
      """
        |select
        |provincename,cityname,
        |sum(case when requestmode = 1 and processnode >=1 then 1 else 0 end) ysrequest,
        |sum(case when requestmode = 1 and processnode >=2 then 1 else 0 end) yxrequest,
        |sum(case when requestmode = 1 and processnode >=3 then 1 else 0 end) adrequest,
        |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) cybid,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) cybidsuccess,
        |sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) show,
        |sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) click,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice/1000 else 0 end) dspWinPrice,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end) dspadpayment
        |from
        |log
        |group by
        |provincename,cityname
      """.stripMargin).show()

  }
}
