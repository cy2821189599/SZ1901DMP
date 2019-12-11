package com.tags

import com.util.TagsUtils
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

import scala.io.Source

object TagContext2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("graphCompute").master("local").getOrCreate()
    val rowDF = spark.read.parquet("D:\\temp\\parquet")
    //停用的关键词
    val stopWords = Source.fromFile("D:\\ReciveFile\\项目2\\项目day10-画像\\笔记\\Spark用户画像分析\\stopwords.txt").getLines()
      .toArray.map(_.trim)
    import spark.implicits._
    val baseRdd = rowDF.filter(TagsUtils.oneUserId).map(row => {
      val userId = TagsUtils.getUserIds(row)
      (userId, row)
    })

    // 创建点
    val VD = baseRdd.rdd.flatMap(r => {
      val row = r._2
      // 广告标签
      val adTag = TagsAD.makeTags(row)
      // 商圈标签
      val businessList = BusinessTag.makeTags(row)
      //app名称标签
      val appName = AppNameTags.makeTags(row)
      //渠道标签
      val platformTag = ChannelTags.makeTags(row)
      //设备标签
      val deviceTags = DeviceTags.makeTags(row)
      // 关键字标签
      val keyWordsTag = KeywordTags.makeTags(row, stopWords)
      // 地域标签
      val areaTags = AreaTags.makeTags(row)
      var vd = r._1.map((_, 0)) ++ adTag ++ businessList ++ appName ++ platformTag ++ deviceTags ++ keyWordsTag ++ areaTags
      r._1.map(userId => {
        if (r._1.head.equals(userId))
          (userId.hashCode.toLong, vd)
        else
          (userId.hashCode.toLong, List.empty)
      })
    })

    // 创建边
    val ED = baseRdd.rdd.flatMap(r => r._1.map(u => Edge(u.hashCode.toLong, r._1.head.hashCode.toLong,0)))

    val graph = Graph(VD,ED)
    graph

    spark.stop()
  }

}
