package com.tags

import com.util.Tags
import org.apache.spark.sql.Row

object ChannelTags extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {
    val row = args(0).asInstanceOf[Row]
    var list = List[(String, Int)]()
    val platform = row.getAs[Int]("adplatformproviderid")
    list :+= ("CN" + platform, 1)
    list
  }

}
