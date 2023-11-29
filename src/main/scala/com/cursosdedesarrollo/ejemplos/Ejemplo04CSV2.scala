package com.cursosdedesarrollo.ejemplos

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object Ejemplos04CSV2 {
  def main(args: Array[String]): Unit = {
    //Reducir el número de LOG
    Logger.getLogger("org").setLevel(Level.OFF)
    //Creando el contexto del Servidor
    val sc = new SparkContext("local","Ejemplo04CSV", System.getenv("SPARK_HOME"))
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CargaJSON")
      .config("log4j.rootCategory", "ERROR, console")
      .getOrCreate()
    val rdd= spark.read.format("csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ";")
      .load("resources/people2.csv")
    val df = rdd.toDF()
    println("Show")
    df.show()
    println("Schema")
    df.printSchema()
    println("col number -> int")
    df.col("number").cast("int")
    println("Renombrando columnas")
    df.withColumnRenamed("name","Nombre")
    df.show()
    //df.write.format("csv").option("header", "true").option("delimiter", ";").save("resources/salida")
    df.foreach(item => {
      println(item.getString(0))
      println(item.getInt(1))
      println(item.getString(2) )
      val cadena =  item.getString(3).replace(",",".")
      println(cadena)
      val numero = cadena.toFloat
      println(numero)
    })
    df.select("number").show()

  }

}

