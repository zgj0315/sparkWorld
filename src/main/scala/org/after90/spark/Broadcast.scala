package org.after90.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhaogj on 30/03/2017.
  */
object Broadcast {
  def main(args: Array[String]): Unit = {
    val strAppName = "strAppName"
    val strMaster = "local[*]"
    val conf = new SparkConf().setAppName(strAppName).setMaster(strMaster)
    val sc = new SparkContext(conf)
    val broadcastVar = sc.broadcast(Array(1, 2, 3))
    println(broadcastVar.value)
    sc.stop()
  }
}
