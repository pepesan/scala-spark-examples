package com.cursosdedesarrollo.ejemplos

import org.apache.spark.sql.{SparkSession, SaveMode}

object Ejemplo04_HDFS {
  def main(args: Array[String]): Unit = {
    // Crear una sesión de Spark
    val spark = SparkSession
      .builder()
      .master("local")
      //.master("spark://localhost:7077")
      .appName("CargaHDFS")
      .getOrCreate()

    // Configurar la ruta del archivo CSV en HDFS
    // val hdfsCsvPath = "hdfs://namenode:9000/upload/datos.csv"
    val hdfsCsvPath = "hdfs://localhost:9001/upload/datos.csv"

    // Crear un DataFrame de ejemplo para escribir en el archivo CSV
    val data = Seq(
      (1, "John", "Doe"),
      (2, "Jane", "Smith"),
      (3, "Bob", "Johnson")
    )
    val columns = Seq("id", "first_name", "last_name")
    val df = spark.createDataFrame(data).toDF(columns: _*)

    // Escribir el DataFrame en el archivo CSV en HDFS
    df.write
      .mode(SaveMode.Overwrite)  // Puedes cambiar esto según tus necesidades (Append, Ignore, ErrorIfExists)
      .csv(hdfsCsvPath)

    println("Datos escritos en CSV en HDFS")

    // Leer datos desde el archivo CSV en HDFS
    val readDf = spark.read
      .option("header", "true")  // Si el archivo CSV tiene encabezados
      .csv(hdfsCsvPath)

    // Mostrar el DataFrame leído
    println("Datos leídos desde CSV en HDFS:")
    readDf.show()

    // Detener la sesión de Spark
    spark.stop()
  }
}

