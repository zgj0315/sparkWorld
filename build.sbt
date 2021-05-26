import sbt.Keys._

lazy val commonSettings = Seq(
  organization := "org.after90",
  version := "0.1.0",
  scalaVersion := "2.12.10"
)

val spark_core = "org.apache.spark" %% "spark-core" % "3.1.1"
val spark_sql = "org.apache.spark" %% "spark-sql" % "3.1.1"
val spark_mllib = "org.apache.spark" %% "spark-mllib" % "3.1.1"
val hadoop = "org.apache.hadoop" % "hadoop-client" % "3.2.1"
val scalactic = "org.scalactic" %% "scalactic" % "3.2.9"
val scalatest = "org.scalatest" % "scalatest_2.12" % "3.2.9" % "test"
val fastjson = "com.alibaba" % "fastjson" % "1.2.73"

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
