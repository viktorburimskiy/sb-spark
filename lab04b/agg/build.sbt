name := "agg"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"  % "provided"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8"  % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.8" % Test