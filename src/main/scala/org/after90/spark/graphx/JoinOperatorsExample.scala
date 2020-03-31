package org.after90.spark.graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object JoinOperatorsExample {
  def createGraphA(sc: SparkContext): Graph[(String, String), String] = {
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
        Edge(5L, 8L, "pi")
      ))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("Unknown", "Missing")
    // Build the initial Graph
    Graph(users, relationships, defaultUser)
  }

  def createGraphB(sc: SparkContext): Graph[(String, String), String] = {
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("zhaogs", "homemate")), (7L, ("jgonzal", "postdoc")),
        (1L, ("zhaogj", "coder")), (4L, ("fengxj", "student"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 1L, "classmate"), Edge(1L, 4L, "homemember")
      ))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("Unknown", "Missing")
    // Build the initial Graph
    Graph(users, relationships, defaultUser)
  }

  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    //    val graph = GraphLoader.edgeListFile(sc, "data/graphx/edgeList.txt")

    val graphA = createGraphA(sc)
    println("graphA:")
    graphA.triplets.map(triplet =>
      "triplet: srcId:" + triplet.srcId + ", srcAttr:" + triplet.srcAttr + ", attr:" + triplet.attr + ", dstId:" + triplet.dstId + ", dstAttr:" + triplet.dstAttr
    ).collect.foreach(println(_))

    val graphB = createGraphB(sc)
    println("graphB:")
    graphB.triplets.map(triplet =>
      "triplet: srcId:" + triplet.srcId + ", srcAttr:" + triplet.srcAttr + ", attr:" + triplet.attr + ", dstId:" + triplet.dstId + ", dstAttr:" + triplet.dstAttr
    ).collect.foreach(println(_))

    // joinVertices
    val graphJoin = graphA.joinVertices(graphB.vertices)((vid, old, outDeg) => (old._1 + "--" + outDeg._1, old._2 + "--" + outDeg._2))
    println("graphJoin:")
    graphJoin.triplets.map(triplet =>
      "triplet: srcId:" + triplet.srcId + ", srcAttr:" + triplet.srcAttr + ", attr:" + triplet.attr + ", dstId:" + triplet.dstId + ", dstAttr:" + triplet.dstAttr
    ).collect.foreach(println(_))
    // outerJoinVertices
    val outerJoinGraph = graphA.outerJoinVertices(graphB.vertices) {
      (vid, data, optDeg) => optDeg.getOrElse("Unknown")
    }
    println("outerJoinGraph:")
    outerJoinGraph.triplets.map(triplet =>
      "triplet: srcId:" + triplet.srcId + ", srcAttr:" + triplet.srcAttr + ", attr:" + triplet.attr + ", dstId:" + triplet.dstId + ", dstAttr:" + triplet.dstAttr
    ).collect.foreach(println(_))
    sc.stop()
  }
}
