package com.tags

import com.util.Tags
import org.apache.spark.sql.Row

/**
  * 广告类型标签
  */
object TagsAD extends Tags{

  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    // 转换类型
    val row = args(0).asInstanceOf[Row]
    // 获取广告类型ID
    val adTypeId = row.getAs[Int]("adspacetype")
    adTypeId match {
      case v if v>9 =>list:+=("LC"+v,1)
      case v if v>0 && v <=9 =>list:+=("LC0"+v,1)
    }
    val adName = row.getAs[String]("adspacetypename")
    if(!adName.isEmpty){
      list:+=("LN"+adName,1)
    }
    // 返回
    list
  }
}
