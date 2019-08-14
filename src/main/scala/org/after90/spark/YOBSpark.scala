package org.after90.spark

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by zhaogj on 31/03/2017.
  */
object YOBSpark {
  def main(args: Array[String]): Unit = {
    val strAppName = "YOBSpark"
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
    val sc = spark.sparkContext

    val res = sc.wholeTextFiles("./data/names/*.txt", 40)
      .flatMap { case (strFilePath, strContent) => {
        val nFilePathLength = strFilePath.length
        val strYear = strFilePath.substring(nFilePathLength - 8, nFilePathLength - 4)
        val astrLine = strContent.split("\n")
        val nstrLineSize = astrLine.size
        for (i <- 0 until nstrLineSize) {
          astrLine(i) = strYear + "," + astrLine(i)
        }
        astrLine
      }
      }
    // The schema is encoded in a string
    val strSchema = "birth name sex count"
    // Generate the schema based on the string of schema
    val fields = strSchema.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val rowRDD = res.map(x => {
      val astrPart = x.split(",")
      Row(astrPart(0), astrPart(1), astrPart(2), astrPart(3))
    })

    val nameDF = spark.createDataFrame(rowRDD, schema)

    nameDF.createOrReplaceTempView("yob")

    //    spark.sql("SELECT birth, SUM(count) FROM yob GROUP BY birth ORDER BY birth").show()
    //    spark.sql("SELECT sex, SUM(count) FROM yob GROUP BY sex ORDER BY sex").show()
    //spark.sql("SELECT sex, name, SUM(count) FROM yob GROUP BY sex, name ORDER BY sum(CAST(count AS DOUBLE)) DESC").show()
    spark.sql("SELECT SUM(count) FROM yob").show()

    spark.stop()
  }


}
