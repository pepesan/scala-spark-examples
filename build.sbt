ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

val sparkVersion = "3.4.1"
val vegasVersion = "0.3.9"
val bokehVersion = "1.0.4"

lazy val root = (project in file("."))
  .settings(
    name := "scala-spark-examples-3.4.1"
  )

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
// libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion
//libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
// https://mvnrepository.com/artifact/org.scalanlp/breeze
libraryDependencies += "org.scalanlp" %% "breeze" % "2.1.0"
// https://mvnrepository.com/artifact/org.scalanlp/breeze-viz
libraryDependencies += "org.scalanlp" %% "breeze-viz" % "2.1.0"

resolvers += "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"