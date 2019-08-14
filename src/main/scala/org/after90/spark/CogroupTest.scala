package org.after90.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhaogj on 01/03/2017.
  */
object CogroupTest {
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

    //    authKey.foreach { case (key, value) => {
    //      println("netlog " + key + " --- " + value)
    //    }
    //    }

    //    netKey.foreach { case (key, value) => {
    //      println("netlog " + key + " --- " + value)
    //    }
    //    }

    //    netKey.cogroup(authKey).foreach { case (key, (netlog, tel)) => {
    //      println("net cogroup auth " + key + " --- " + netlog + " --- " + tel)
    //    }
    //    }

    //    authKey.cogroup(netKey).foreach { case (key, (netlog, tel)) => {
    //      println("auth cogroup net " + key + " --- " + netlog + " --- " + tel)
    //    }
    //    }

    val result = netKey.cogroup(authKey).map { x => {
      val key = x._1
      val value = x._2
      val compactBufferA = value._1
      val compactBufferB = value._2
      var output = ""
      compactBufferA.map { x => {
        output = output + " -A- " + x
      }
      }
      compactBufferB.map { x => {
        output = output + " -B- " + x
      }
      }
      key + " --- " + output
    }
    }
    result.foreach(x => {
      println(x)
    })

    sc.stop
  }
}
