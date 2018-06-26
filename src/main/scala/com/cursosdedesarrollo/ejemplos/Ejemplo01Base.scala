package com.cursosdedesarrollo.ejemplos


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Ejemplos01Base {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","Ejemplo01Base", System.getenv("SPARK_HOME"))
    // Read the CSV file
    val csv = sc.textFile("resources/sample.csv").cache()
    // split / clean data
    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
    // get header
    val header = headerAndRows.first
    // filter out header (eh. just check if the first val matches the first header name)
    val data = headerAndRows.filter(_(0) != header(0))
    // splits to map (header/value pairs)
    val maps = data.map(splits => header.zip(splits).toMap)
    // filter out the user "me"
    val result = maps.filter(map => map("user") != "me").persist()
    // print result
    result.foreach(println)
  }

}

