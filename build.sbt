ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

val sparkVersion = "3.2.0"
lazy val root = (project in file("."))
  .settings(
    name := "FileProcessingAssessment"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.17.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % "test"

