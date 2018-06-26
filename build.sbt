name := "scala-spark-examples"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.1"


libraryDependencies += "org.apache.spark" % "spark-core_2.11" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % sparkVersion




