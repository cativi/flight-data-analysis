name := "FlightDataAnalysis"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.1" % Test
)

scalacOptions ++= Seq("-Yrangepos")
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

// To use KryoSerializer
run / fork := true
run / javaOptions ++= Seq(
  "-Dspark.serializer=org.apache.spark.serializer.KryoSerializer",
"-Dlog4j.configuration=log4j.properties"
)