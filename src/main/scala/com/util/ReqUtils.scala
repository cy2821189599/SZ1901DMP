package com.util

import scala.collection.mutable.ListBuffer

/**
  * 工具类
  */
object ReqUtils {

  def reqAd(requestmode: Int, processnode: Int,
            iseffective: Int, isbilling: Int, isbid: Int,
            iswin: Int, adorderid: Int, winprice: Double, adpayment: Double) = {
    var list1 = ListBuffer[Double]()
    // 处理请求 三个请求
    if(requestmode == 1 && processnode ==1){
      list1+=(1,0,0)
    }else if(requestmode == 1 && processnode ==2){
      list1+=(1,1,0)
    }else if(requestmode == 1 && processnode ==3){
      list1+=(1,1,1)
    }else{
      list1+=(0,0,0)
    }
    // 处理竞价和成本等指标
    var list2 = ListBuffer[Double]()
    if(iseffective ==1 && isbilling ==1 && isbid==1){
      if(iseffective ==1 && isbilling ==1 && iswin ==1 && adorderid!=0){
        list2+=(1,1,winprice/1000,adpayment/1000)
      }else{
        list2+=(1,0,0,0)
      }
    }else{
      list2+=(0,0,0,0)
    }
    // 处理展示点击
    var list3 = ListBuffer[Double]()
    if(requestmode ==2 && iseffective ==1){
      list3+=(1,0)
    }else if(requestmode ==3 && iseffective ==1){
      list3+=(0,1)
    }else{
      list3+=(0,0)
    }
    // 返回
    list1++list2++list3
  }
}


