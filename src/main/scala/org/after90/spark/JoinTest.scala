package org.after90.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhaogj on 01/03/2017.
  */
object JoinTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JoinTest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val authRDD = sc.textFile("./data/authlog.txt")
    val netRDD = sc.textFile("./data/netlog.txt")

    val authKey = authRDD.map(line => {
      val parts = line.split("\t")
      (parts(0), parts(1))
    })

    val netKey = netRDD.map(line => {
      val parts = line.split("\t")
      (parts(0), line)
    })

    authKey.foreach { case (key, value) => {
      println("netlog " + key + " --- " + value)
    }
    }

    netKey.foreach { case (key, value) => {
      println("netlog " + key + " --- " + value)
    }
    }

    netKey.join(authKey).foreach { case (key, (netlog, tel)) => {
      println("net join auth " + key + " --- " + netlog + " --- " + tel)
    }
    }

    authKey.join(netKey).foreach { case (key, (netlog, tel)) => {
      println("auth join net " + key + " --- " + netlog + " --- " + tel)
    }
    }

    sc.stop
  }
}
