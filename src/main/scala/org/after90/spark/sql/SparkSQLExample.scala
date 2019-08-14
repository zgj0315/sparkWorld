package org.after90.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object SparkSQLExample {
  case class Person(name: String, age: Long)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val peopleDF = spark.sparkContext
      .textFile("src/main/resources/people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    peopleDF.createOrReplaceTempView("people")
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
  }
}
