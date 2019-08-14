package org.after90.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by zhaogj on 30/03/2017.
  */
object BroadcastTest {
  def main(args: Array[String]): Unit = {
    val blockSize = if (args.length > 2) args(2) else "4096"
    val strAppName = "InitSparkAndSession"
    //方便调试，如果运行在集群上，需要将master设置为yarn-cluster
    var strMaster = "local[*]"
    if (args.length == 1) {
      strMaster = args(0)
    }
    val conf = new SparkConf().setAppName(strAppName).setMaster(strMaster)
    val sc = new SparkContext(conf)
    //必须先初始化sc,后初始化session,奇了个怪了
    val spark = SparkSession
      .builder()
      .master(strMaster)
      .appName(strAppName)
      .config("spark.broadcast.blockSize", blockSize)
      .getOrCreate()


    val slices = if (args.length > 0) args(0).toInt else 2
    val num = if (args.length > 1) args(1).toInt else 1000000

    val arr1 = (0 until num).toArray

    for (i <- 0 until 3) {
      println("Iteration " + i)
      println("===========")
      val startTime = System.nanoTime
      val barr1 = sc.broadcast(arr1)
      val observedSizes = sc.parallelize(1 to 10, slices).map(_ => barr1.value.length)
      // Collect the small RDD so we can print the observed sizes locally.
      observedSizes.collect().foreach(j => println(i+","+j))
      println("Iteration %d took %.0f milliseconds".format(i, (System.nanoTime - startTime) / 1E6))
    }

    spark.stop()
  }
}
