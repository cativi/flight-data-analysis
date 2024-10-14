name := "FlightDataAnalysis"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.10"

val sparkVersion = "2.4.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.1" % Test
)

scalacOptions ++= Seq("-Yrangepos")

fork := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")

run / javaOptions ++= Seq(
  "-Dspark.serializer=org.apache.spark.serializer.KryoSerializer",
  "-Dlog4j.configuration=log4j.properties"
)

// Conservative dependency overrides
dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.3",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7.1"
)

// Disable parallel execution for tests
Test / parallelExecution := false

// Check Java version
initialize := {
  val _ = initialize.value // run the previous initialization
  val required = "1.8"
  val current  = sys.props("java.specification.version")
  assert(current == required, s"Unsupported JDK: java.specification.version $current != $required")
}