name := "testProject"

version := "1.0"

scalaVersion := "2.11.8"



// https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.0" % "provided"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.0.0" % "provided"