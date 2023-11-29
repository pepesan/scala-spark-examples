package com.cursosdedesarrollo.ejemplos

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, SparkSession, functions}

object Ejemplos05_03_Procesado {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local","Ejemplo05Procesado", System.getenv("SPARK_HOME"))
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CargaJSON")
      .getOrCreate()
    val data = Seq(
      Person("Alice", 25),
      Person("Bob", 30),
      Person("Charlie", 35),
      Person("Alice", 40)
    )
    val df = spark.createDataFrame(data)

    // Utilizando funcionalidades de columnas
    val modifiedDF = df.withColumn("age_plus_two", addTwoToAge(df("age")))
      .withColumn("name_uppercase", uppercaseName(df("name")))
      .withColumn("age_category", categorizeAge(df("age")))

    modifiedDF.show()

      // Cierra la sesión de Spark al finalizar
      spark.stop()
    }


    def addTwoToAge(column: Column): Column = {
      column + 2
    }

    def uppercaseName(column: Column): Column = {
      functions.upper(column)
    }

    def categorizeAge(column: Column): Column = {
      functions.when(column < 30, "Young")
        .when(column < 40, "Middle-aged")
        .otherwise("Elderly")
    }


}

