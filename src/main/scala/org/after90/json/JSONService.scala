package org.after90.json

import org.apache.spark.sql.SparkSession

object JSONService {
  def main(args: Array[String]): Unit = {
    val strAppName = "JSONService"
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
    //val sc = spark.sparkContext
    val df_ip = spark.read.json("./data/asset_ip.json")
    df_ip.createOrReplaceTempView("asset_ip")
    val df_ip_update = spark.read.json("./data/asset_ip_update.json")
    df_ip_update.createOrReplaceTempView("asset_ip_update")
    import spark.sql
    println(sql("SELECT * FROM asset_ip r RIGHT JOIN asset_ip_update l ON r.ip = l.ip").show())
    //sc.stop()
    spark.stop()
  }
}
