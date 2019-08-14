package org.after90.spark

/**
  * Created by zhaogj on 30/12/2016.
  * spark-submit --master yarn-cluster --class org.after90.spark.WordCount sbtprojecttest_2.10-0.1.0.jar
  */

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val textRDD = sc.textFile("./data/wordCount.txt")
    val wordCount = textRDD.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    println(wordCount)
  }
}
