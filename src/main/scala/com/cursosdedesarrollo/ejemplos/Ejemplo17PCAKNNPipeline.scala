package com.cursosdedesarrollo.ejemplos

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
//import org.apache.spark.ml.classification.KNNClassifier

object Ejemplo17PCAKNNPipeline {
  Logger.getLogger("org").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
    //Creando el contexto del Servidor
    val sc = new SparkContext("local","Ejemplo01Base", System.getenv("SPARK_HOME"))
    sc.setLogLevel("ERROR")
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CargaJSON")
      .getOrCreate()
    /*
    //read in raw label and features
    val rawDataset = MLUtils.loadLibSVMFile(sc, "data/mnist/mnist.bz2")
      .toDF()
    // convert "features" from mllib.linalg.Vector to ml.linalg.Vector
    val dataset = MLUtils.convertVectorColumnsToML(rawDataset)

    //split training and testing
    val Array(train, test) = dataset
      .randomSplit(Array(0.7, 0.3), seed = 1234L)
      .map(_.cache())

    //create PCA matrix to reduce feature dimensions
    val pca = new PCA()
      .setInputCol("features")
      .setK(50)
      .setOutputCol("pcaFeatures")
    val knn = new KNNClassifier()
      .setTopTreeSize(dataset.count().toInt / 500)
      .setFeaturesCol("pcaFeatures")
      .setPredictionCol("predicted")
      .setK(1)
    val pipeline = new Pipeline()
      .setStages(Array(pca, knn))
      .fit(train)

    val insample = validate(pipeline.transform(train))
    val outofsample = validate(pipeline.transform(test))

    //reference accuracy: in-sample 95% out-of-sample 94%
    logger.info(s"In-sample: $insample, Out-of-sample: $outofsample")
    */
  }
  /*
  private[this] def validate(results: DataFrame): Double = {
    results
      .selectExpr("SUM(CASE WHEN label = predicted THEN 1.0 ELSE 0.0 END) / COUNT(1)")
      .collect()
      .head
      .getDecimal(0)
      .doubleValue()
  }
  */
}

