package org.after90.spark.scala

import org.apache.spark.sql.SparkSession

/**
  * Created by zhaogj on 18/05/2017.
  */
object ReadFile {
  def main(args: Array[String]): Unit = {
    val strAppName = "ReadFile"
    //方便调试，如果运行在集群上，需要将master设置为yarn-cluster
    var strMaster = "local[*]"
    if (args.length == 1) {
      strMaster = args(0)
    }
    val spark = SparkSession
      .builder()
      .master(strMaster)
      .appName(strAppName)
      .getOrCreate()
    val data = spark.read.parquet("./data/part-00000")
    //spark.read.format("parquet").load("./data/part-00000")
    //data.show()
    //    data.printSchema()
    //    data.foreach(x => {
    //      println(x)
    //    })
    //
    val dataRdd = data.rdd
    dataRdd.map(x => "" + x).filter(_.contains("a961.w57.w.cucdn.cn")).foreach(println)
    //data.rdd.filter(_.contains("1491840023"))

    spark.stop()
  }
}
