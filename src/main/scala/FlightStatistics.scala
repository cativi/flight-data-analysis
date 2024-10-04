import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.sql.Date
import org.apache.spark.storage.StorageLevel

case class Flight(passengerId: String, flightId: String, from: String, to: String, date: String)

case class Passenger(passengerId: String, firstName: String, lastName: String)

case class FlightCount(month: Int, numberOfFlights: Long)

case class FrequentFlyer(passengerId: String, numberOfFlights: Long, firstName: String, lastName: String)

case class CountryRun(passengerId: String, longestRun: Long)

case class PassengerPair(passenger1: String, passenger2: String, flightsTogether: Long)

object FlightStatistics {

  def calculateFlightsPerMonth(flightDS: Dataset[Flight])(implicit encoder: Encoder[FlightCount]): Dataset[FlightCount] = {
    flightDS
      .filter(col("date").isNotNull)
      .groupBy(month(to_date(col("date"), "yyyy-MM-dd")).as("month"))
      .agg(count("*").as("numberOfFlights"))
      .as[FlightCount]
      .orderBy("month")
  }

  def findFrequentFlyers(flightDS: Dataset[Flight], passengersDS: Dataset[Passenger], limit: Int)(implicit encoder: Encoder[FrequentFlyer]): Dataset[FrequentFlyer] = {
    flightDS
      .groupBy("passengerId")
      .agg(count("*").as("numberOfFlights"))
      .join(passengersDS, Seq("passengerId"), "left")
      .select(
        col("passengerId"),
        col("numberOfFlights"),
        coalesce(col("firstName"), lit("Unknown")).as("firstName"),
        coalesce(col("lastName"), lit("Unknown")).as("lastName")
      )
      .as[FrequentFlyer]
      .orderBy(col("numberOfFlights").desc)
      .limit(limit)
  }

  def findLongestNonUKRun(flightDS: Dataset[Flight])(implicit encoder: Encoder[CountryRun]): Dataset[CountryRun] = {
    val countryRunsDF = flightDS
      .select(col("passengerId"), to_date(col("date"), "yyyy-MM-dd").as("date"), col("from").as("country"))
      .union(flightDS.select(col("passengerId"), to_date(col("date"), "yyyy-MM-dd").as("date"), col("to").as("country")))
      .distinct()
      .orderBy("passengerId", "date")
      .persist(StorageLevel.MEMORY_AND_DISK)

    val windowSpec = Window.partitionBy("passengerId").orderBy("date")

    val result = countryRunsDF
      .withColumn("uk_visit", when(lower(col("country")) === "uk", 1).otherwise(0))
      .withColumn("run_id", sum("uk_visit").over(windowSpec))
      .groupBy("passengerId", "run_id")
      .agg(countDistinct("country").as("countries_visited"))
      .groupBy("passengerId")
      .agg(max("countries_visited").as("longestRun"))
      .as[CountryRun]
      .orderBy(col("longestRun").desc)

    countryRunsDF.unpersist()
    result
  }

  def findPassengersFlownTogether(flightDS: Dataset[Flight], minFlightsTogether: Int)(implicit encoder: Encoder[PassengerPair]): Dataset[PassengerPair] = {
    flightDS
      .filter(col("date").isNotNull)
      .as("f1")
      .join(flightDS.filter(col("date").isNotNull).as("f2"),
        col("f1.date") === col("f2.date") &&
        col("f1.flightId") === col("f2.flightId") &&
        col("f1.passengerId") < col("f2.passengerId"))
      .groupBy(col("f1.passengerId").as("passenger1"), col("f2.passengerId").as("passenger2"))
      .agg(count("*").as("flightsTogether"))
      .filter(col("flightsTogether") >= minFlightsTogether)
      .as[PassengerPair]
      .orderBy(col("flightsTogether").desc)
  }

  def flownTogether(flightDS: Dataset[Flight], atLeastNTimes: Int, from: Date, to: Date)(implicit encoder: Encoder[PassengerPair]): Dataset[PassengerPair] = {
    val filteredFlights = flightDS.filter(to_date(col("date"), "yyyy-MM-dd").between(from, to))

    filteredFlights.as("f1")
      .join(filteredFlights.as("f2"),
        col("f1.date") === col("f2.date") &&
        col("f1.flightId") === col("f2.flightId") &&
        col("f1.passengerId") < col("f2.passengerId"))
      .groupBy(col("f1.passengerId").as("passenger1"), col("f2.passengerId").as("passenger2"))
      .agg(count("*").as("flightsTogether"))
      .filter(col("flightsTogether") >= atLeastNTimes)
      .as[PassengerPair]
      .orderBy(col("flightsTogether").desc)
  }

 def writeToCSV[T](ds: Dataset[T], path: String): Unit = {
    ds.write
      .mode("overwrite")  // This will overwrite the existing directory
      .option("header", "true")
      .csv(path)
  }
}