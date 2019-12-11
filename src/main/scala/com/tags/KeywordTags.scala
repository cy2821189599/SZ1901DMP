package com.tags

import com.util.Tags
import org.apache.spark.sql.Row

import scala.swing.event.AdjustingEvent

object KeywordTags extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var keywordList: List[(String, Int)] = List()
    val row = args(0).asInstanceOf[Row]
    val stopWords = args(1).asInstanceOf[Array[String]]
    val keyWords = row.getAs[String]("keywords")
    if (!keyWords.isEmpty) {
      val words = keyWords.split("\\|")
      words.foreach(w => {
        if (w.length >= 3 && w.length <= 8 && !stopWords.contains(w)) {
          keywordList :+= ("K" + w, 1)
        }
      })
    }
    keywordList
  }
}
