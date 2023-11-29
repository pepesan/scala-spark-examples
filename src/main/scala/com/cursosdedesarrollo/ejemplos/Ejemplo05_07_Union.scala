package com.cursosdedesarrollo.ejemplos

import com.cursosdedesarrollo.ejemplos.entities.Department
import com.cursosdedesarrollo.ejemplos.entities.Employee
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, SparkSession, functions}


object Ejemplos05_07_Union {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "Ejemplo05Procesado", System.getenv("SPARK_HOME"))
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CargaJSON")
      .getOrCreate()
    // Crear DataFrame 1
    val df1 = spark.createDataFrame(Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie")
    )).toDF("id", "name")

    // Crear DataFrame 2
    val df2 = spark.createDataFrame(Seq(
      (4, "David"),
      (5, "Eva")
    )).toDF("id", "name")

    // Realizar la unión de los DataFrames
    val result = df1.union(df2)

    // Mostrar el resultado
    result.show()

    // Cierra la sesión de Spark al finalizar
    spark.stop()
  }
  

}

