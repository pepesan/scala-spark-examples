package com.cursosdedesarrollo.ejemplos

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, SparkSession, functions}

object Ejemplos05_04_MapReduce {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      "local",
      "Ejemplo05Procesado",
      System.getenv("SPARK_HOME"))
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CargaJSON")
      .getOrCreate()
    // Crea un RDD a partir de una lista de enteros
    val inputList = List(1, 2, 3, 4, 5)
    val inputRDD = spark.sparkContext.parallelize(inputList)

    // Map: Transforma cada elemento del RDD en un par (clave, valor)
    val mappedRDD = inputRDD.map(value => (value % 2, value))

    // Reduce: Aplica una función de reducción para combinar los valores con la misma clave
    val reducedRDD = mappedRDD.reduceByKey(
      /*
        Java Puro y Duro
        public Data calculateSumatory(Data acc, Data act){
          return acc+act;
        }
        Java Lambda
        (Data acc, Data act) => {
          return acc + act;
        }
        Java Lambda Corta
        (acc, act) => return acc+act;

        Salto cualitativo

        Scala Lambda Corta
        (_, _) => return _ + _ ;
        (_, _) => _ + _
       */
        _ + _
    )

    // Recopila los resultados
    val result = reducedRDD.collect()

    // Imprime los resultados
    result.foreach(println)

    // Cierra la sesión de Spark al finalizar
    spark.stop()
  }
  

}

