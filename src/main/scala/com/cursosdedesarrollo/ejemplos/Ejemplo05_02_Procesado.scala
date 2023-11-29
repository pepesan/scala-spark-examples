package com.cursosdedesarrollo.ejemplos

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, functions}

object Ejemplos05_02_Procesado {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","Ejemplo05Procesado", System.getenv("SPARK_HOME"))
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CargaJSON")
      .getOrCreate()
    import spark.implicits._
    val data = Seq(
      Person("Alice", 25),
      Person("Bob", 30),
      Person("Charlie", 35),
      Person("Alice", 40)
    )
    val df = spark.createDataFrame(data)

    // Muestra el contenido del DataFrame
    df.show()

    // Filtra los registros donde la edad sea mayor o igual a 30
    val filteredDF = df.filter(functions.col("age") >= 30)
    filteredDF.show()

    // Agrupa por nombre y calcula el promedio de edad
    val avgAgeDF = df.groupBy("name")
      .agg(functions.avg("age")
        .alias("avg_age"))
    avgAgeDF.show()

    // Ordena por edad de forma descendente
    val sortedDF = df.orderBy(functions.col("age").desc)
    sortedDF.show()

    // Calcula la suma de las edades
    val sumOfAges = df.agg(functions.sum("age")).head().getLong(0)
    println(s"Sum of ages: $sumOfAges")

    // Cierra la sesión de Spark al finalizar
    spark.stop()
  }

}

