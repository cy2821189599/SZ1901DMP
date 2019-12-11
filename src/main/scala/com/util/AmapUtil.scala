package com.util

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.mutable.ListBuffer

/**
  * 从高德获取商圈信息
  */
object AmapUtil {

  def getBusinessFromAMap(long:Double,lat:Double):String={
    // 拼接经纬度
    val location = long+","+lat
    //获取url
    val urlStr = "https://restapi.amap" +
      ".com/v3/geocode/regeo?location="+location+"&key=5386c60b89ab66747cc385cca1aaca90&radius=3000"
    // 获取http请求
    val json = HttpUtil.get(urlStr)
    // 解析
    val jsonObj = JSON.parseObject(json)
    // 判断状态是否成功
    val status = jsonObj.getIntValue("status")
    if(status == 0) return ""
    // 继续解析
    val regeocodeJson = jsonObj.getJSONObject("regeocode")
    if(regeocodeJson == null || regeocodeJson.keySet().isEmpty){
      return ""
    }
    val addJson = regeocodeJson.getJSONObject("addressComponent")
    if(addJson == null || addJson.keySet().isEmpty){
      return ""
    }
    val arr = addJson.getJSONArray("businessAreas")
    if(arr == null || arr.isEmpty){
      return ""
    }
    var list = ListBuffer[String]()
    for (item<-arr.toArray){
      if(item.isInstanceOf[JSONObject]){
        val json = item.asInstanceOf[JSONObject]
        val name = json.getString("name")
        list.append(name)
      }
    }
    list.mkString(",")
  }

}
