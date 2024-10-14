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
    import flightDS.sparkSession.implicits._

    flightDS
      .filter($"date".isNotNull)
      .groupBy(month($"date").as("month"))
      .agg(countDistinct($"flightId").as("numberOfFlights"))
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
    import flightDS.sparkSession.implicits._

    val flightsWithUKIndicator = flightDS
      .withColumn("isUK", lower(col("from")).contains("uk") || lower(col("to")).contains("uk"))

    val flightsWithUKFlag = flightsWithUKIndicator
      .withColumn("isUKSegment", when(col("isUK"), lit(1)).otherwise(lit(0)))

    val windowSpec = Window.partitionBy("passengerId").orderBy("date")
    val flightsWithSegmentId = flightsWithUKFlag
      .withColumn("nonUKSegmentId", sum("isUKSegment").over(windowSpec))

    val nonUKFlights = flightsWithSegmentId.filter(!col("isUK"))

    val segmentCountriesVisited = nonUKFlights
      .groupBy("passengerId", "nonUKSegmentId")
      .agg(countDistinct("to").as("countriesVisited"))

    val longestNonUKRun = segmentCountriesVisited
      .groupBy("passengerId")
      .agg(max("countriesVisited").as("longestRun"))
      .as[CountryRun]
      .orderBy(col("longestRun").desc)

    longestNonUKRun
  }

  def findPassengersFlownTogether(flightDS: Dataset[Flight], minFlightsTogether: Int)(implicit encoder: Encoder[PassengerPair]): Dataset[PassengerPair] = {
    import flightDS.sparkSession.implicits._

    flightDS
      .as("f1")
      .join(flightDS.as("f2"),
        col("f1.flightId") === col("f2.flightId") &&
        col("f1.passengerId") < col("f2.passengerId")
      )
      .groupBy(col("f1.passengerId").as("passenger1"), col("f2.passengerId").as("passenger2"))
      .agg(count("*").as("flightsTogether"))
      .filter(col("flightsTogether") > minFlightsTogether)
      .distinct()
      .as[PassengerPair]
      .orderBy(col("flightsTogether").desc)
  }

  def flownTogether(flightDS: Dataset[Flight], atLeastNTimes: Int, from: Date, to: Date)(implicit encoder: Encoder[PassengerPair]): Dataset[PassengerPair] = {
    import flightDS.sparkSession.implicits._

    // Ensure the date format matches the timestamp format in the dataset
    val filteredFlights = flightDS
      .withColumn("flightDate", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
      .filter(col("flightDate").geq(from) && col("flightDate").leq(to))

    // Logging the count to verify if records are filtered correctly
    val filteredCount = filteredFlights.count()
    println(s"Filtered flights within range ($from to $to): $filteredCount")

    if (filteredCount == 0) {
      println("No flights found in the given date range.")
      return flightDS.sparkSession.emptyDataset[PassengerPair]
    }

    // Cache the dataset to improve performance for repeated operations
    val cachedFlights = filteredFlights.cache()

    // Join and group to find pairs of passengers who have flown together
    val passengerPairs = cachedFlights
      .as("f1")
      .join(cachedFlights.as("f2"),
        col("f1.flightId") === col("f2.flightId") &&
        col("f1.passengerId") < col("f2.passengerId")
      )
      .groupBy(col("f1.passengerId").as("passenger1"), col("f2.passengerId").as("passenger2"))
      .agg(count("*").as("flightsTogether"))
      .filter(col("flightsTogether") >= atLeastNTimes)
      .as[PassengerPair]
      .orderBy(col("flightsTogether").desc)

    passengerPairs
  }

  def writeToCSV[T](ds: Dataset[T], path: String): Unit = {
    ds.write
      .mode("overwrite")
      .option("header", "true")
      .csv(path)
  }
}
