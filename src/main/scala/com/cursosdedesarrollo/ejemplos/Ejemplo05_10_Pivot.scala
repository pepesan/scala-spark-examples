package com.cursosdedesarrollo.ejemplos

import com.cursosdedesarrollo.ejemplos.entities.EmployeeTransformable
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession, functions}


object Ejemplos05_10_Pivot {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "Ejemplo05Procesado", System.getenv("SPARK_HOME"))
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CargaJSON")
      .getOrCreate()
    val data = Seq(("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"),
      ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
      ("Carrots", 1200, "China"), ("Beans", 1500, "China"), ("Orange", 4000, "China"),
      ("Banana", 2000, "Canada"), ("Carrots", 2000, "Canada"), ("Beans", 2000, "Mexico"))

    import spark.sqlContext.implicits._
    val df = data.toDF("Product", "Amount", "Country")
    df.show()

    val pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
    pivotDF.show()

    val countries = Seq("USA", "China", "Canada", "Mexico")
    val pivotDF2 = df.groupBy("Product").pivot("Country", countries).sum("Amount")
    pivotDF2.show()

    // Unpivot
    val unPivotDF = pivotDF.select($"Product",
      expr("stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"))
      .where("Total is not null")
    unPivotDF.show()


    // Cierra la sesión de Spark al finalizar
    spark.stop()
  }
  

}

