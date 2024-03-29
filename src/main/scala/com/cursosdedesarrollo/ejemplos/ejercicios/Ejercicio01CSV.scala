package com.cursosdedesarrollo.ejemplos.ejercicios

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


object Ejercicio01CSV {
  def main(args: Array[String]): Unit = {
    //Reducir el número de LOG
    Logger.getLogger("org").setLevel(Level.OFF)
    //Creando el contexto del Servidor
    val sc = new SparkContext("local","Ejemplo04CSV", System.getenv("SPARK_HOME"))
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CargaCSV")
      .config("log4j.rootCategory", "ERROR, console")
      .getOrCreate()
    val df = spark.read.format("csv").option("header", "true")
      .option("inferSchema", "true").option("delimiter", ";")
      .load("resources/salary.csv").toDF()
    println("Show")
    df.show()
    println("Schema")
    df.printSchema()
  }

}

