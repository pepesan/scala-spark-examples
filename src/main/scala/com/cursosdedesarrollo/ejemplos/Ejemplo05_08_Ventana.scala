package com.cursosdedesarrollo.ejemplos

import com.cursosdedesarrollo.ejemplos.entities.Sale
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, SparkSession, functions}


object Ejemplos05_08_Ventana {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "Ejemplo05Procesado", System.getenv("SPARK_HOME"))
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CargaJSON")
      .getOrCreate()
    val data = Seq(
      Sale("2022-01-01", "A", 100),
      Sale("2022-01-02", "A", 200),
      Sale("2022-01-03", "A", 150),
      Sale("2022-01-01", "B", 300),
      Sale("2022-01-02", "B", 250),
      Sale("2022-01-03", "B", 180)
    )
    val df = spark.createDataFrame(data)

    // Define una ventana por producto y orden por fecha
    val windowSpec = Window.partitionBy("product").orderBy("date")

    // Utiliza funciones de ventana para realizar cálculos en el conjunto de filas
    val result = df.withColumn("row_number", functions.row_number().over(windowSpec))
      .withColumn("rank", functions.rank().over(windowSpec))
      .withColumn("revenue_lag", functions.lag("revenue", 1).over(windowSpec))
      .withColumn("revenue_lead", functions.lead("revenue", 1).over(windowSpec))

    // Muestra los resultados
    result.show()

    // Cierra la sesión de Spark al finalizar
    spark.stop()
  }
  

}

