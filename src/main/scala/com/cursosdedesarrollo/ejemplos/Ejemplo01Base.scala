package com.cursosdedesarrollo.ejemplos


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
object Ejemplo01Base {
  def main(args: Array[String]): Unit = {
    //Creando el contexto del Servidor
    val conf = new SparkConf().setAppName("Ejemplo01Base")
      //.setMaster("local")
      .setMaster("spark://localhost:7077")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      // .master("local")
      .master("spark://localhost:7077")
      .appName("CargaJSON")
      .getOrCreate()
    /*
    val sc = new SparkContext("local",
      "Ejemplo01Base",
      System.getenv("SPARK_HOME"))
    sc.setLogLevel("ERROR")
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CargaJSON")
      .getOrCreate()

     */
  }

}

