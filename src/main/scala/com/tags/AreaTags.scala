package com.tags

import com.util.Tags
import org.apache.spark.sql.Row

object AreaTags extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var areaList: List[(String, Int)] = List()
    val row = args(0).asInstanceOf[Row]
    val province = row.getAs[String]("provincename")
    val city = row.getAs[String]("cityname")
    areaList :+= (province + "-" + city, 1)
    areaList
  }
}
