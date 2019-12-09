package com.appRequest

import com.util.ReqUtils
import org.apache.spark.sql.SparkSession

/**
  * core
  */
object LocationV2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("location").master("local").getOrCreate()
    // 获取路径
    val Array(inputPath,outpuPath)=args
    // 获取数据
    val df = spark.read.parquet(inputPath)
    // 转换RDD
    df.rdd.map(row=>{
      val provincename = row.getAs[String]("provincename")
      val cityname = row.getAs[String]("cityname")
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
      val reqList = ReqUtils.reqAd(requestmode,processnode,iseffective,
        isbilling,isbid,iswin,adorderid,winprice,adpayment)
      ((provincename,cityname),reqList)
    }).reduceByKey((list1,list2)=>{
      // list1(1,2,3,4) list2(1,2,3,4) zip(List((1,1),(2,2),(3,3),(4,4)))
      list1.zip(list2)
        // List((1+1),(2+2),(3+3),(4+4))
        .map(t=>t._1+t._2)
      // List(2,4,6,8)
    }).map(t=>t._1+" : "+t._2.mkString("<",",",">")).foreach(println)
  }
}
