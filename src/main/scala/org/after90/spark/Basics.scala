package org.after90.spark

import org.after90.spark.utils.StringUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhaogj on 30/03/2017.
  */
object Basics {
  def main(args: Array[String]): Unit = {
    val strAppName = "strAppName"
    val strMaster = "local[*]"
    val conf = new SparkConf().setAppName(strAppName).setMaster(strMaster)
    val sc = new SparkContext(conf)
    val lines = sc.textFile("./data/wordCount.txt")
    val lineLengths = lines.map(s => StringUtil.myTrim(s).length)
    val totalLength = lineLengths.reduce((a, b) => a + b)
    lineLengths.persist()
    println(totalLength)
    sc.stop()
  }
}
