
name := "ScalaProj2"

version := "0.1"

scalaVersion := "2.12.12"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.1"
// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
//libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.1.1" % "provided"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.1"
libraryDependencies += "org.xerial" % "sqlite-jdbc" % "3.20.0"
//libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.1.1"
//libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.2"