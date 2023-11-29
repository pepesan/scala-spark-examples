package com.cursosdedesarrollo.ejemplos

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler

object RegresionLinealSpark {

  def main(args: Array[String]): Unit = {

    // Configuración de Spark
    val conf = new SparkConf().setAppName("RegresionLinealSpark").setMaster("local")
    val spark = SparkSession.builder.config(conf).getOrCreate()

    // Lectura del archivo CSV
    val data = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("resources/regresion_lineal.csv")

    // Preprocesamiento de datos
    val assembler = new VectorAssembler().setInputCols(Array("HorasEstudio")).setOutputCol("features")
    val assembledData = assembler.transform(data)

    // Definición del modelo de regresión lineal
    val lr = new LinearRegression().setLabelCol("Calificación").setFeaturesCol("features")

    // División del conjunto de datos en conjunto de entrenamiento y prueba
    val Array(trainingData, testData) = assembledData.randomSplit(Array(0.8, 0.2), seed = 1234)

    // Entrenamiento del modelo
    val model = lr.fit(trainingData)

    // Realización de predicciones en el conjunto de prueba
    val predictions = model.transform(testData)

    // Mostrar resultados
    predictions.select("HorasEstudio", "Calificación", "prediction").show()

    // Evaluación del modelo (puedes calcular métricas de error aquí)

    // Guardar el modelo entrenado
    model.write.overwrite().save("modelos_salida/regresion_lineal")

    // Detener la sesión de Spark
    spark.stop()
  }
}
