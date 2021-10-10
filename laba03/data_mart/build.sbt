name := "data_mart"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"  % "provided"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8"  % "provided"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.5.2"
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "7.15.0"
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.24"
