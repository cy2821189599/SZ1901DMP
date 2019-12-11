package com.tags

import java.io.IOException

import com.util.TagsUtils
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
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

    val configuration = spark.sparkContext.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum", "namenode:2181,secondnamenode:2181,datanode1:2181")
    val connection = ConnectionFactory.createConnection(configuration)
    val admin = connection.getAdmin
//    if(!admin.tableExists(TableName.valueOf("ns1:sz1901"))){
//      val htd = new HTableDescriptor(TableName.valueOf("ns1:sz1901"))
//      val hcd = new HColumnDescriptor("tags")
//      hcd.setBloomFilterType(BloomType.ROW)
//      hcd.setTimeToLive(24 * 60 * 60)
//      htd.addFamily(hcd)
//      try admin.createTable(htd)
//      catch {
//        case e: IOException =>
//          e.printStackTrace()
//      }
//    }else{
//      println("表已存在")
//    }

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
    val integration: RDD[(String, List[(String, Int)])] = transform.rdd.reduceByKey((list1: List[(String, Int)], list2: List[(String, Int)]) =>
      list1 ++ list2.groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2))
    )

    // 保存到hbase
    var list = new java.util.ArrayList[Put]
    try {
      val table = connection.getTable(TableName.valueOf("ns1:sz1901"))
      integration.foreach(r => {
        val put = new Put(r._1.getBytes)
        //行键下有列族，将数据插入到对应列族中的列限定符下
        put.addColumn(Bytes.toBytes("tags"), Bytes.toBytes("2019-12-11"), Bytes.toBytes(r._2.toBuffer.toString()))
//              table.put(put)
        list.add(put)
      })
      // 插入多条数据
      table.put(list)
      println(list.size()+"\tOK")
    } catch {
      case e: IOException =>
        e.printStackTrace()
    } finally {
      admin.close()
      connection.close()
    }
    spark.stop()

  }
}
