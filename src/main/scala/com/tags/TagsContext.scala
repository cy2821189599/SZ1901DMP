package com.tags

import com.util.TagsUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.io.Source

/**
 * 上下标签
 */
object TagsContext {
  def main(args: Array[String]): Unit = {

    val Array(inputPath, outputPath) = args
    val spark = SparkSession.builder().appName("tags").master("local").getOrCreate()

    // 获取数据
    val df = spark.read.parquet(inputPath)
    //停用的关键词
    val stopWords = Source.fromFile("D:\\ReciveFile\\项目2\\项目day10-画像\\笔记\\Spark用户画像分析\\stopwords.txt").getLines()
      .toArray.map(_.trim)
    // 打标签
    import spark.implicits._
    val transform = df
      // 过滤符合的用户数据
      .filter(TagsUtils.oneUserId)
      .map(row => {
        // 拿到UserID
        val userId = TagsUtils.getOneUserID(row)
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
        //         上下文标签
        (userId, adTag ++ businessList ++ appName ++ platformTag ++ deviceTags ++ keyWordsTag ++ areaTags)
      })
    transform.rdd.reduceByKey((list1: List[(String, Int)], list2: List[(String, Int)]) =>
      list1 ++ list2.groupBy(_._1).mapValues(_.foldLeft(0)(_+_._2))
    )
    spark.stop()

  }
}
