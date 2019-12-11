package com.tags

import com.util.Tags
import org.apache.spark.sql.Row

object DeviceTags extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var devList: List[(String, Int)] = List()
    val row = args(0).asInstanceOf[Row]
    val os = row.getAs[Int]("client")
    os match {
      case 1 => devList :+= ("D00010001", 1)
      case 2 => devList :+= ("D00010002", 1)
      case 3 => devList :+= ("D00010003", 1)
      case _ => devList :+= ("D00010004", 1)
    }
    val network = row.getAs[String]("networkmannername")
    network match {
      case "Wifi" => devList :+= ("D00020001", 1)
      case "4G" => devList :+= ("D00020002", 1)
      case "3G" => devList :+= ("D00020003", 1)
      case "2G" => devList :+= ("D00020004", 1)
      case _ => devList :+= ("D00020005", 1)
    }
    val isp = row.getAs[String]("ispname")
    isp match {
      case "移动" => devList :+= ("D00030001", 1)
      case "联通" => devList :+= ("D00030002", 1)
      case "电信" => devList :+= ("D00030003", 1)
      case _ => devList :+= (" D00030004", 1)
    }
    devList
  }
}
