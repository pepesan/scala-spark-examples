package com.cursosdedesarrollo.ejemplos

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.util.Random

object Ejemplo21HDFS {
  def main(args: Array[String]): Unit = {
    val host = "localhost"
    val port = "9001" // debería ser 9000 de normal
    val path= "/user/admin/"


    // generación de npombre de fichero random
    val random = new Random()
    val length = 8
    val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')

    val randomString = (1 to length)
      .map(_ => chars(random.nextInt(chars.length)))
      .mkString
    val filename = "prueba-"+randomString+".csv"
    //"hdfs dfs -rm -r /user/admin/prueba.csv" !
    val sc = new SparkContext("local",
      "Ejemplo01Base",
      System.getenv("SPARK_HOME"))
    sc.setLogLevel("ERROR")
    // Grabar un RDD en HDFS
    val rdd = sc.parallelize(List(
      (0, 60),
      (0, 56),
      (0, 54),
      (0, 62),
      (0, 61),
      (0, 53),
      (0, 55),
      (0, 62),
      (0, 64),
      (1, 73),
      (1, 78),
      (1, 67),
      (1, 68),
      (1, 78)
    ))
    rdd.saveAsTextFile(
      "hdfs://"+host+":"+port+path+filename)
    rdd.collect
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CargaJSON")
      .config("log4j.rootCategory", "ERROR, console")
      .getOrCreate()


    val df = spark.read.format("csv").option("header", "true")

      .option("inferSchema", "true").option("delimiter", ",").load("hdfs://"+host+":"+port+path+filename).toDF()
    val dfc=df.collect()
    dfc.foreach { element =>
      println(element)
    }

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    fs.delete(new Path("file://"+path+filename),true)
    //fs.removeAcl(new Path("file://"+path+filename))
  }

}
