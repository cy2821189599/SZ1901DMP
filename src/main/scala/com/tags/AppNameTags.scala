package com.tags

import com.util.{RedisUtil, Tags}
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object AppNameTags extends Tags {

  override def makeTags(args: Any*): List[(String, Int)] = {
    val row = args(0).asInstanceOf[Row]
    var list = List[(String, Int)]()
    val appid = row.getAs[String]("appid")
    var appname = row.getAs[String]("appname")
    // 判断当前APPName是否为空
    if (appname.isEmpty) {
      var jedis: Jedis = null
      try {
        jedis = RedisUtil.getConnection()
        appname = jedis.get(appid)
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        jedis.close()
      }
    }
    list :+= ("APP" + appname, 1)
    list
  }
}
