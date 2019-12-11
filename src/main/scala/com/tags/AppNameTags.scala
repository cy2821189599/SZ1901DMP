package com.tags

import com.util.RedisUtil
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object AppNameTags {

  def getTag(row: Row) = {
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
    ("APP"+appname,1)
  }


}
