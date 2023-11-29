package com.cursosdedesarrollo.ejemplos

import com.cursosdedesarrollo.ejemplos.entities.EmployeeTransformable
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession, functions}


object Ejemplos05_09_Transform {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "Ejemplo05Procesado", System.getenv("SPARK_HOME"))
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CargaJSON")
      .getOrCreate()
    val data = Seq(
      EmployeeTransformable("Alice", 25, 50000, true),
      EmployeeTransformable("Bob", 30, 60000, false),
      EmployeeTransformable("Charlie", 35, 55000, true),
      EmployeeTransformable("David", 40, 65000, true),
      EmployeeTransformable("Eva", 45, 70000, false)
    )
    // Crear el DataFrame de ejemplo
    val ventas = Seq(
      ("2023-07-01", "Producto A", 3, 10.5),
      ("2023-07-01", "Producto B", 2, 8.0),
      ("2023-07-02", "Producto A", 1, 10.5),
      ("2023-07-02", "Producto C", 4, 12.75),
      ("2023-07-03", "Producto B", 5, 8.0)
    )


    val df = spark.createDataFrame(ventas)
    val columnas = Seq("fecha", "producto", "cantidad", "precio_unitario")
    val dfConNombres = columnas.zip(df.schema.fieldNames).foldLeft(df) { (acc, columnas) =>
      acc.withColumnRenamed(columnas._2, columnas._1)
    }
    // Agregar la columna "monto_total" calculando la multiplicación de "cantidad" por "precio_unitario"
    val ventasConMontoTotal = dfConNombres.withColumn("monto_total", col("cantidad") * col("precio_unitario"))

    // Mostrar el resultado
    ventasConMontoTotal.show()

    // Cierra la sesión de Spark al finalizar
    spark.stop()
  }
  

}

