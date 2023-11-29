package com.cursosdedesarrollo.ejemplos

import com.cursosdedesarrollo.ejemplos.entities.Employee
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, SparkSession, functions}


object Ejemplos05_05_Aggregation {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "Ejemplo05Procesado", System.getenv("SPARK_HOME"))
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CargaJSON")
      .getOrCreate()
    val data = Seq(
      Employee("IT", "Alice", 50000),
      Employee("IT", "Bob", 60000),
      Employee("HR", "Charlie", 55000),
      Employee("HR", "David", 65000)
    )
    val df = spark.createDataFrame(data)

    // Realiza la operación de groupBy y las agregaciones
    val result = df.groupBy("department")
      .agg(
        functions.sum("salary").alias("total_salary"),
        functions.avg("salary").alias("avg_salary"),
        functions.min("salary").alias("min_salary"),
        functions.max("salary").alias("max_salary")
      )

    // Muestra los resultados
    result.show()

    // Cierra la sesión de Spark al finalizar
    spark.stop()
  }
  

}

