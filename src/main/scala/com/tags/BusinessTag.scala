package com.tags

import ch.hsr.geohash.GeoHash
import com.util.{AmapUtil, JedisConn, Tags}
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object BusinessTag extends Tags {

  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    // 获取经纬度
    val long = row.getAs[String]("long")
    val lat = row.getAs[String]("lat")
    // 通过经纬度获取商圈
    val business = getBusiness(long, lat)
    if (business != null && !business.isEmpty && business.length > 0) {
      val lines = business.split(",")
      lines.foreach(t => {
        list :+= (t, 1)
      })
    }
    list
  }

  /**
   * 获取商圈
   *
   * @param long
   * @param lat
   * @return
   */
  def getBusiness(long: String, lat: String): String = {
    var latDouble: Double = 0.0
    var longDouble: Double = 0.0
    try {
      latDouble = lat.toDouble
      longDouble = long.toDouble
    } catch {
      case e:Exception => return null
    }
    val geoHash = GeoHash.geoHashStringWithCharacterPrecision(latDouble, longDouble, 6)
    // 去查询数据库
    var str = redis_queryBusiness(geoHash)
    // 查询高德
    if (str == null) {
      str = AmapUtil.getBusinessFromAMap(longDouble, latDouble)
      // 存储到redis
      if (str == null){
        str = ""
      }
      redis_insertBusiness(geoHash, str)
    }
    str
  }

  /**
   * 查询数据库
   *
   * @param geoHash
   * @return
   */
  def redis_queryBusiness(geoHash: String): String = {
    var jedis:Jedis = null
    var str:String = null
    try {
      jedis = JedisConn.getConn()
      str = jedis.get(geoHash)
    } catch {
      case e:Exception =>e.printStackTrace()
    } finally {
      jedis.close()
    }
    str
  }

  /**
   * 将数据存储redis
   *
   * @param geoHash
   * @param str
   * @return
   */
  def redis_insertBusiness(geoHash: String, str: String) = {
    var jedis:Jedis = null
    try {
      jedis = JedisConn.getConn()
      jedis.set(geoHash, str)
    } catch {
      case e:Exception =>e.printStackTrace()
    } finally {
      jedis.close()
    }
  }

}
