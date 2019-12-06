package com.log

import com.util.{SchemaType, StrUtils}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 将数据格式转换为Parquet格式
  */
object log2Parquet {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    val spark = SparkSession.builder()
      .appName("parquet")
      .master("local")
      // 设置序列化方式
      // .config("","")
      .getOrCreate()
    // 判断路径
    if(args.length != 2){
      println("目录不正确，退出")
      sys.exit()
    }
    // 获取数据
    val Array(inputPath,outputPath) =args
    val lines = spark.sparkContext.textFile(inputPath)
    // 先切分在过滤
    val rowRDD = lines.map(t=>t.split(",",-1)).filter(_.length>=85).map(arr=>{
      Row(
        arr(0),
        StrUtils.toInt(arr(1)),
        StrUtils.toInt(arr(2)),
        StrUtils.toInt(arr(3)),
        StrUtils.toInt(arr(4)),
        arr(5),
        arr(6),
        StrUtils.toInt(arr(7)),
        StrUtils.toInt(arr(8)),
        StrUtils.toDouble(arr(9)),
        StrUtils.toDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        StrUtils.toInt(arr(17)),
        arr(18),
        arr(19),
        StrUtils.toInt(arr(20)),
        StrUtils.toInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        StrUtils.toInt(arr(26)),
        arr(27),
        StrUtils.toInt(arr(28)),
        arr(29),
        StrUtils.toInt(arr(30)),
        StrUtils.toInt(arr(31)),
        StrUtils.toInt(arr(32)),
        arr(33),
        StrUtils.toInt(arr(34)),
        StrUtils.toInt(arr(35)),
        StrUtils.toInt(arr(36)),
        arr(37),
        StrUtils.toInt(arr(38)),
        StrUtils.toInt(arr(39)),
        StrUtils.toDouble(arr(40)),
        StrUtils.toDouble(arr(41)),
        StrUtils.toInt(arr(42)),
        arr(43),
        StrUtils.toDouble(arr(44)),
        StrUtils.toDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        StrUtils.toInt(arr(57)),
        StrUtils.toDouble(arr(58)),
        StrUtils.toInt(arr(59)),
        StrUtils.toInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        StrUtils.toInt(arr(73)),
        StrUtils.toDouble(arr(74)),
        StrUtils.toDouble(arr(75)),
        StrUtils.toDouble(arr(76)),
        StrUtils.toDouble(arr(77)),
        StrUtils.toDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        StrUtils.toInt(arr(84))
      )
    })
    // 构建DF
    val df = spark.createDataFrame(rowRDD,SchemaType.structType)
    // 保存数据结果
    df.write.parquet(outputPath)
    // 关闭
    spark.stop()

  }
}
