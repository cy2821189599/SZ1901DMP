package com.tags

import ch.hsr.geohash.GeoHash
import com.util.{AmapUtil, JedisConn, Tags}
import org.apache.spark.sql.Row

object BusinessTag extends Tags{

  override def makeTags(args: Any*): List[(String, Int)] = {

    var list =List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    // 获取经纬度
    val long = row.getAs[String]("long")
    val lat = row.getAs[String]("lat")
    // 通过经纬度获取商圈
    val business = getBusiness(long,lat)
    val lines = business.split(",")
    lines.foreach(t=>{
      list:+=(t,1)
    })
    list
  }

  /**
    * 获取商圈
    * @param long
    * @param lat
    * @return
    */
  def getBusiness(long: String, lat: String): String = {
    val geoHash = GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble,long.toDouble,6)
      // 去查询数据库
      var str = redis_queryBusiness(geoHash)
      // 查询高德
      if(str==null||str.length==0){
        str = AmapUtil.getBusinessFromAMap(long.toDouble,lat.toDouble)
        // 存储到redis
        redis_insertBusiness(geoHash,str)
      }
      str
  }

  /**
    * 查询数据库
    * @param geoHash
    * @return
    */
  def redis_queryBusiness(geoHash: String): String = {
    val jedis = JedisConn.getConn()
    val str = jedis.get(geoHash)
    str
  }

  /**
    * 将数据存储redis
    * @param geoHash
    * @param str
    * @return
    */
  def redis_insertBusiness(geoHash: String, str: String) = {
    val jedis = JedisConn.getConn()
    jedis.set(geoHash,str)
  }

}
