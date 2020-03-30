package org.after90.spark.graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object StructuralOperatorsExample {
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
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
        (4L, ("peter", "student"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
        Edge(4L, 0L, "student"), Edge(5L, 0L, "colleague")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    // Notice that there is a user 0 (for which we have no information) connected to users
    // 4 (peter) and 5 (franklin).
    //    graph.triplets.map(
    //      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    //    ).collect.foreach(println(_))
    //     Remove missing vertices as well as the edges to connected to them
    //    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    // The valid subgraph will disconnect users 4 and 5 by removing user 0
    //    validGraph.vertices.collect.foreach(println(_))
    //    validGraph.triplets.map(
    //      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    //    ).collect.foreach(println(_))

    //show graph
    println("graph:")
    graph.triplets.map(triplet =>
      "srcId:" + triplet.srcId + ", srcAttr:" + triplet.srcAttr + ", attr:" + triplet.attr + ", dstId:" + triplet.dstId + ", dstAttr:" + triplet.dstAttr
    ).collect.foreach(println(_))

    //得到方向相反的图
    val graphR = graph.reverse
    //show graph
    println("graphR:")
    graphR.triplets.map(triplet =>
      "srcId:" + triplet.srcId + ", srcAttr:" + triplet.srcAttr + ", attr:" + triplet.attr + ", dstId:" + triplet.dstId + ", dstAttr:" + triplet.dstAttr
    ).collect.foreach(println(_))

    //    println("validGraph:")
    //    validGraph.triplets.map(triplet =>
    //      "srcId:" + triplet.srcId + ", srcAttr:" + triplet.srcAttr + ", attr:" + triplet.attr + ", dstId:" + triplet.dstId + ", dstAttr:" + triplet.dstAttr
    //    ).collect.foreach(println(_))

    // Run Connected Components
    // 返回一个新的图，去掉节点的属性
    val ccGraph = graph.connectedComponents() // No longer contains missing field
    println("ccGraph:")
    ccGraph.triplets.map(triplet =>
      "srcId:" + triplet.srcId + ", srcAttr:" + triplet.srcAttr + ", attr:" + triplet.attr + ", dstId:" + triplet.dstId + ", dstAttr:" + triplet.dstAttr
    ).collect.foreach(println(_))

    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    println("validGraph:")
    validGraph.triplets.map(triplet =>
      "srcId:" + triplet.srcId + ", srcAttr:" + triplet.srcAttr + ", attr:" + triplet.attr + ", dstId:" + triplet.dstId + ", dstAttr:" + triplet.dstAttr
    ).collect.foreach(println(_))

    // Restrict the answer to the valid subgraph
    // 两个图的交集子图
    val validCCGraph = ccGraph.mask(validGraph)
    println("validCCGraph:")
    validCCGraph.triplets.map(triplet =>
      "srcId:" + triplet.srcId + ", srcAttr:" + triplet.srcAttr + ", attr:" + triplet.attr + ", dstId:" + triplet.dstId + ", dstAttr:" + triplet.dstAttr
    ).collect.foreach(println(_))



    sc.stop()
  }
}
