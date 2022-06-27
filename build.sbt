name := "scala-spark-examples"

version := "0.1"

scalaVersion := "2.12.8"

val sparkVersion = "3.3.0"
val vegasVersion = "0.3.9"
val bokehVersion = "1.0.4"




// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
// libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib

libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"


//libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % sparkVersion
libraryDependencies += "org.scalanlp" %% "breeze" % "1.1"
libraryDependencies += "org.scalanlp" %% "breeze-viz" % "1.1"
resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
/*
libraryDependencies  ++= Seq(
  // Last stable release
  "org.scalanlp" %% "breeze" % "0.13.2",

  // Native libraries are not included by default. add this if you want them (as of 0.7)
  // Native libraries greatly improve performance, but increase jar sizes.
  // It also packages various blas implementations, which have licenses that may or may not
  // be compatible with the Apache License. No GPL code, as best I know.
  "org.scalanlp" %% "breeze-natives" % "0.13.2",

  // The visualization library is distributed separately as well.
  // It depends on LGPL code
  "org.scalanlp" %% "breeze-viz" % "0.13.2"
)
/*
resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.6")
libraryDependencies += "saurfang" %% "spark-knn" % "0.2.0"
*/
libraryDependencies += "org.diana-hep" %% "histogrammar" % "1.0.4"


resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"



