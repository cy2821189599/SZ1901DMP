package com.tags

import com.util.TagsUtils
import org.apache.spark.sql.SparkSession

/**
  * 上下标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {

    val Array(inputPath,outputPath)=args
    val spark = SparkSession.builder().appName("tags").master("local").getOrCreate()

    // 获取数据
    val df = spark.read.parquet(inputPath)
    // 打标签
    df
      // 过滤符合的用户数据
      .filter(TagsUtils.oneUserId)
      .map(row=>{
        // 拿到UserID
        val userId = TagsUtils.getOneUserID(row)
        // 广告标签
        val adTag = TagsAD.makeTags(row)
        // 商圈标签
        BusinessTag.makeTags(row)
    })

  }
}
