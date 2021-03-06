package org.after90.spark.graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId, VertexRDD}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object AggregateMessagesExample {
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

    // $example on$
    // Create a graph with "age" as the vertex property.
    // Here we use a random graph for simplicity.
    val graph: Graph[Double, Int] =
    GraphGenerators.logNormalGraph(sc, numVertices = 100).mapVertices((id, _) => id.toDouble)

    //    println(s"numE:${graph.numEdges}, numV:${graph.numVertices}")

    //show graph
    //    graph.triplets.map(triplet =>
    //      "srcAttr:" + triplet.srcAttr + ", attr:" + triplet.attr + ", dstAttr:" + triplet.dstAttr
    //    ).collect.foreach(println(_))

    // Compute the number of older followers and their total age
    val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
      triplet => { // Map Function
        //        println(s"src:${triplet.srcAttr}, dst:${triplet.dstAttr}")
        if (triplet.srcAttr > triplet.dstAttr) {
          // Send message to destination vertex containing counter and age
          triplet.sendToDst((1, triplet.srcAttr))
        }
      },
      // Add counter and age
      (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )
    // Divide total age by number of older followers to get average age of older followers
    val avgAgeOfOlderFollowers: VertexRDD[Double] =
      olderFollowers.mapValues((id, value) =>
        value match {
          case (count, totalAge) => totalAge / count
        })
    // Display the results
    avgAgeOfOlderFollowers.collect.foreach(println(_))
    // $example off$
    val graphA = createGraphA(sc)
    println("graphA:")
    graphA.triplets.map(triplet =>
      "triplet: srcId:" + triplet.srcId + ", srcAttr:" + triplet.srcAttr + ", attr:" + triplet.attr + ", dstId:" + triplet.dstId + ", dstAttr:" + triplet.dstAttr
    ).collect.foreach(println(_))
    val graphAgregateMessages = graphA.aggregateMessages[(String, String)](
      triplet => { // Map Function
        triplet.sendToDst(triplet.srcAttr._1 + "--" + triplet.dstAttr._1, triplet.srcAttr._1 + "--" + triplet.dstAttr._2)
      },
      (a, b) => (a._1 + "++" + b._1, a._2 + "++" + b._2) // Reduce Function
    )
    graphAgregateMessages.collect.foreach(println(_))


    //    val graphNeighborIds = graphA.collectNeighborIds(EdgeDirection.In)
    //    val graphNeighborIds = graphA.collectNeighborIds(EdgeDirection.Out)
    val graphNeighborIds = graphA.collectNeighborIds(EdgeDirection.Either)

    println("graphNeighborIds:")
    graphNeighborIds.foreach(line => {
      print(line._1 + ":");
      for (elem <- line._2) {
        print(elem + " ")
      };
      println;
    })


    spark.stop()
  }
}
