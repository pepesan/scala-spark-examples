package com.cursosdedesarrollo.ejemplos

import org.apache.spark.sql.SparkSession

object Ejemplo00SesionRemota {
  // ejecuta del docker compose up -d
  def main(args: Array[String]): Unit = {
    // Crea una instancia de SparkSession
    val spark = SparkSession
      .builder()
      .appName("MiAppSpark")
      .master("spark://localhost:7077") // URL del Spark Master
      .getOrCreate()

    // Código de tu aplicación Spark

    // Cierra la sesión de Spark al finalizar
    spark.stop()
  }
}
