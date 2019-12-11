package com.graphx

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

/**
  * 图计算例子
  */
object GraphxTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("graph").master("local").getOrCreate()
    // 构建点和边
    // 构建点
    val vdRDD = spark.sparkContext.makeRDD(Seq(
      (1L,("张三",23)),
      (2L,("阿奎罗",33)),
      (6L,("内马尔",27)),
      (9L,("马塞洛",35)),
      (133L,("库蒂尼奥",36)),
      (5L,("席尔瓦",35)),
      (7L,("法尔考",39)),
      (158L,("张智",39)),
      (16L,("苏亚雷斯",30)),
      (138L,("武磊",28)),
      (21L,("J罗",23)),
      (44L,("李四",30))
    ))
    // 构建边
    val edgeRDD  = spark.sparkContext.makeRDD(Seq(
      Edge(1L,133L,0),
      Edge(2L,133L,0),
      Edge(6L,133L,0),
      Edge(9L,133L,0),
      Edge(16L,138L,0),
      Edge(6L,138L,0),
      Edge(21L,138L,0),
      Edge(44L,138L,0),
      Edge(5L,158L,0),
      Edge(7L,158L,0)
    ))
    // 构建图
    val graph = Graph(vdRDD,edgeRDD)
    // 取出所有连接的顶点
    val vd = graph.connectedComponents().vertices
    vd.foreach(println)
    // 整理数据
    vd.join(vdRDD).map{
      case (userid,(vd,(name,age))) =>(vd,List((name,age)))
    }.reduceByKey(_++_).foreach(println)
  }
}
