package org.after90.spark.graphx

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PropertyGraphExample {
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
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    //    println(s"numE:${graph.numEdges}, numV:${graph.numVertices}")

    //show graph
    graph.triplets.map(triplet =>
      "srcId:" + triplet.srcId + ", srcAttr:" + triplet.srcAttr + ", attr:" + triplet.attr + ", dstId:" + triplet.dstId + ", dstAttr:" + triplet.dstAttr
    ).collect.foreach(println(_))


    // Count all users which are postdocs
    //    println(graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count)
    // Count all the edges where src > dst
    //    println(graph.edges.filter(e => e.srcId > e.dstId).count)

    val inDegrees: VertexRDD[Int] = graph.inDegrees
    inDegrees.map(x =>
      "v:" + x._1 + ",inDegrees:" + x._2
    ).collect.foreach(println(_))

    val outDegrees: VertexRDD[Int] = graph.outDegrees
    outDegrees.map(x =>
      "vId:" + x._1 + ", outDegrees:" + x._2
    ).collect.foreach(println(_))

    val degrees: VertexRDD[Int] = graph.degrees
    degrees.map(x =>
      "vId:" + x._1 + ", degrees:" + x._2
    ).collect.foreach(println(_))

    // alter vertices
    val newGraphV = graph.mapVertices {
      case (id, attr) => (attr._1 + "name", attr._2 + "role")
    }
    //show graph
    //    newGraphV.triplets.map(triplet =>
    //      "srcAttr:" + triplet.srcAttr + ", attr:" + triplet.attr + ", dstAttr:" + triplet.dstAttr
    //    ).collect.foreach(println(_))

    // alter edges
    val newGraphE = graph.mapEdges(e => e.attr + "role")
    //        show graph
    //    newGraphE.triplets.map(triplet =>
    //      "srcAttr:" + triplet.srcAttr + ", attr:" + triplet.attr + ", dstAttr:" + triplet.dstAttr
    //    ).collect.foreach(println(_))

    // alter triplets
    val newGraphT = graph.mapTriplets(t => t.attr + "role")

    //    newGraphT.triplets.map(triplet =>
    //      "srcAttr:" + triplet.srcAttr + ", attr:" + triplet.attr + ", dstAttr:" + triplet.dstAttr
    //    ).collect.foreach(println(_))

    // Given a graph where the vertex property is the out degree
    val inputGraph: Graph[Int, String] =
      graph.outerJoinVertices(graph.outDegrees)((vid, _, degOpt) => degOpt.getOrElse(0))

    //show graph
    inputGraph.triplets.map(triplet =>
      "srcId:" + triplet.srcId + ", srcAttr:" + triplet.srcAttr + ", attr:" + triplet.attr + ", dstId:" + triplet.dstId + ", dstAttr:" + triplet.dstAttr
    ).collect.foreach(println(_))

    //outerJoinVertices
    //创建点RDD
    val usersVertices: RDD[(VertexId, (String, String))] = sc.parallelize(Array(
      (1L, ("Spark", "scala")), (2L, ("Hadoop", "java")),
      (3L, ("Kafka", "scala")), (4L, ("Zookeeper", "Java "))))
    //创建边RDD
    val usersEdges: RDD[Edge[String]] = sc.parallelize(Array(
      Edge(2L, 1L, "study"), Edge(3L, 2L, "train"),
      Edge(1L, 2L, "exercise"), Edge(4L, 1L, "None")))

    val salaryVertices: RDD[(VertexId, (String, Long))] = sc.parallelize(Array(
      (1L, ("Spark", 30L)), (2L, ("Hadoop", 15L)),
      (3L, ("Kafka", 10L)), (5L, ("parameter server", 40L))
    ))
    val salaryEdges: RDD[Edge[String]] = sc.parallelize(Array(
      Edge(2L, 1L, "study"), Edge(3L, 2L, "train"),
      Edge(1L, 2L, "exercise"), Edge(5L, 1L, "None")))

    //构造Graph
    val graphUser = Graph(usersVertices, usersEdges)
    val graphSalary = Graph(salaryVertices, salaryEdges)
    //outerJoinVertices操作,
    val joinGraph = graphUser.outerJoinVertices(graphSalary.vertices) { (id, attr, deps) =>
      deps match {
        case Some(deps) => deps
        case None => 0
      }
    }
    //show graph
    joinGraph.triplets.map(triplet =>
      "srcId:" + triplet.srcId + ", srcAttr:" + triplet.srcAttr + ", attr:" + triplet.attr + ", dstId:" + triplet.dstId + ", dstAttr:" + triplet.dstAttr
    ).collect.foreach(println(_))
    joinGraph.vertices.collect.foreach(println)

    //得到方向相反的图
    val graphR = graph.reverse
    //show graph
    graphR.triplets.map(triplet =>
      "srcId:" + triplet.srcId + ", srcAttr:" + triplet.srcAttr + ", attr:" + triplet.attr + ", dstId:" + triplet.dstId + ", dstAttr:" + triplet.dstAttr
    ).collect.foreach(println(_))

    sc.stop()
  }
}
