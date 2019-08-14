package org.after90.spark

import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by zhaogj on 21/03/2017.
  * 读取目标文件为xml格式的文件，解析xml，输出bcp格式
  */
object ReadXml {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ReadXml").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val xmlRDD = sc.textFile("./data/fairscheduler.xml")

    /**
      * 控制文件不被分片的前提下，手工组合出bcp文件
      */

    xmlRDD.foreach { x => {
      println(x)
    }
    }

    sc.stop()

  }
}
