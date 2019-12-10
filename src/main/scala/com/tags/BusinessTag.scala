package com.tags

import com.util.Tags
import org.apache.spark.sql.Row

object BusinessTag extends Tags{

  override def makeTags(args: Any*): List[(String, Int)] = {

    var list =List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    // 获取经纬度
    val long = row.getAs[String]("long")
    val lat = row.getAs[String]("lat")
    // 通过经纬度获取商圈
    getBusiness(long,lat)
  }

  def getBusiness(long: String, lat: String): String = {

  }

}
