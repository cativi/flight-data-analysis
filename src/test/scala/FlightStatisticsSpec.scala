import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}
import org.apache.spark.sql.functions._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import java.sql.Date
import FlightStatistics._

object FlightStatisticsSpec {
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("FlightStatisticsTest")
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.default.parallelism", "1")
    .getOrCreate()
}

class FlightStatisticsSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  import FlightStatisticsSpec.spark
  import spark.implicits._

  // Test data creation
  val testFlights: Dataset[Flight] = Seq(
    Flight("P1", "F1", Date.valueOf("2023-01-01"), "A", "B"),
    Flight("P2", "F1", Date.valueOf("2023-01-01"), "A", "B"),
    Flight("P2", "F2", Date.valueOf("2023-01-15"), "B", "C"),
    Flight("P3", "F3", Date.valueOf("2023-02-01"), "C", "D"),
    Flight("P1", "F4", Date.valueOf("2023-01-15"), "B", "C"),
    Flight("P2", "F5", Date.valueOf("2023-02-15"), "D", "E"),
    Flight("P4", "F6", null, "E", "F") // Test null date
  ).toDS()

  val testPassengers: Dataset[Passenger] = Seq(
    Passenger("P1", "John", "Doe"),
    Passenger("P2", "Jane", "Smith"),
    Passenger("P3", "Alice", "Johnson"),
    Passenger("P5", "Bob", "Brown") // Passenger with no flights
  ).toDS()

  override def afterAll(): Unit = {
    spark.stop()
  }

 "calculateFlightsPerMonth" should "correctly count flights per month and handle null dates" in {
    val result = FlightStatistics.calculateFlightsPerMonth(testFlights)
      .collect()
      .toSet

    result should contain theSameElementsAs Set(
      FlightCount(1, 4),
      FlightCount(2, 2)
    )
  }

  "findFrequentFlyers" should "correctly identify frequent flyers and handle passengers with no flights" in {
    val result = FlightStatistics.findFrequentFlyers(testFlights, testPassengers, 5)
      .collect()
      .toSet

    result should contain theSameElementsAs Set(
      FrequentFlyer("P2", 3, "Jane", "Smith"),
      FrequentFlyer("P1", 2, "John", "Doe"),
      FrequentFlyer("P3", 1, "Alice", "Johnson"),
      FrequentFlyer("P4", 1, "Unknown", "Unknown")
    )
  }

  "findLongestNonUKRun" should "correctly identify the longest non-UK run" in {
    val specialFlights = Seq(
      Flight("P1", "F1", Date.valueOf("2023-01-01"), "FR", "DE"),
      Flight("P1", "F2", Date.valueOf("2023-01-02"), "DE", "IT"),
      Flight("P1", "F3", Date.valueOf("2023-01-03"), "IT", "UK"),
      Flight("P1", "F4", Date.valueOf("2023-01-04"), "UK", "ES"),
      Flight("P2", "F5", Date.valueOf("2023-01-01"), "US", "CA"),
      Flight("P2", "F6", Date.valueOf("2023-01-02"), "CA", "MX"),
      Flight("P2", "F7", Date.valueOf("2023-01-03"), "MX", "uk") // Test case-insensitivity
    ).toDS()

    val result = FlightStatistics.findLongestNonUKRun(specialFlights)
      .collect()
      .toSet

    result should contain theSameElementsAs Set(
      CountryRun("P1", 3),
      CountryRun("P2", 3)
    )
  }

  "findPassengersFlownTogether" should "correctly identify passengers who have flown together" in {
    val result = FlightStatistics.findPassengersFlownTogether(testFlights, 1)
      .collect()
      .toSet

    result should contain theSameElementsAs Set(
      PassengerPair("P1", "P2", 1)
    )
  }

  "flownTogether" should "correctly identify passengers who have flown together within a date range" in {
    val result = FlightStatistics.flownTogether(
      testFlights,
      1,
      Date.valueOf("2023-01-01"),
      Date.valueOf("2023-01-31")
    ).collect().toSet

    result should contain theSameElementsAs Set(
      PassengerPair("P1", "P2", 1)
    )
  }

  it should "handle empty date ranges" in {
    val result = FlightStatistics.flownTogether(
      testFlights,
      1,
      Date.valueOf("2022-01-01"),
      Date.valueOf("2022-12-31")
    ).collect()

    result should be (empty)
  }

   "calculateFlightsPerMonth" should "handle dataset with only null dates" in {
    val nullFlights = Seq(
      Flight("P1", "F1", null, "A", "B"),
      Flight("P2", "F2", null, "B", "C")
    ).toDS()

    val result = FlightStatistics.calculateFlightsPerMonth(nullFlights)
      .collect()

    result should be (empty)
  }
}