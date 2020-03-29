package org.after90.spark.graphx

import org.apache.spark.graphx.{Graph, VertexRDD}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql.SparkSession

object AggregateMessagesExample {
  def main(args: Array[String]): Unit = {
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

    println(s"numE:${graph.numEdges}, numV:${graph.numVertices}")

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
    //    avgAgeOfOlderFollowers.collect.foreach(println(_))
    // $example off$

    spark.stop()
  }
}
