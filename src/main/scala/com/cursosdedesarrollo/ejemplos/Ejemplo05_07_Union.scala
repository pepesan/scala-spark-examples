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
    val employeeData = Seq(
      Employee("IT", "Alice"),
      Employee("HR", "Bob"),
      Employee("Finance", "Charlie")
    )
    val departmentData = Seq(
      Department("IT", "New York"),
      Department("HR", "London"),
      Department("Sales", "Paris")
    )
    val employeeDF = spark.createDataFrame(employeeData)
    val departmentDF = spark.createDataFrame(departmentData)

    // Realiza la operación de join
    val joinedDF = employeeDF.join(departmentDF, Seq("department"), "inner")

    // Muestra los resultados
    joinedDF.show()

    // Cierra la sesión de Spark al finalizar
    spark.stop()
  }
  

}

