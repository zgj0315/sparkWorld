import sbt.Keys._

lazy val commonSettings = Seq(
  organization := "org.after90",
  version := "0.1.0",
  scalaVersion := "2.11.12"
)

val spark_core = "org.apache.spark" % "spark-core_2.11" % "2.2.1"
val spark_sql = "org.apache.spark" % "spark-sql_2.11" % "2.2.1"
val spark_mllib = "org.apache.spark" % "spark-mllib_2.11" % "2.2.1"
val hadoop = "org.apache.hadoop" % "hadoop-client" % "2.7.3"
val scalactic = "org.scalactic" %% "scalactic" % "3.0.1"
val scalatest = "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test"
val fastjson = "com.alibaba" % "fastjson" % "1.2.59"

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "SparkWorld",
    libraryDependencies += spark_core,
    libraryDependencies += spark_sql,
    libraryDependencies += spark_mllib,
    libraryDependencies += hadoop,
    libraryDependencies += scalactic,
    libraryDependencies += scalatest,
    libraryDependencies += fastjson
  )
