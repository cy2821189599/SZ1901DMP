package com.tags

import org.apache.spark.sql.Row

object ChannelTags {
  def getchannel(row: Row) = {
    val platform = row.getAs[Int]("adplatformproviderid")
    ("CN" + platform, 1)
  }

}
