import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, Dataset}
import java.sql.Date
import FlightStatistics._

object FlightDataAnalysisApp {
def main(args: Array[String]): Unit = {
    // Set the log level
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("FlightDataAnalysis")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._

    try {
      val flightDataPath = "/Users/carlostv2021/Desktop/Development/Quantexa/Flight Data Assignment/flightData.csv"
      val passengerDataPath = "/Users/carlostv2021/Desktop/Development/Quantexa/Flight Data Assignment/passengers.csv"

      println(s"Reading flight data from: $flightDataPath")
      val flightsDF = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(flightDataPath)

      println("Flight Data Schema:")
      flightsDF.printSchema()
      println("Flight Data Sample:")
      flightsDF.show(5, false)

      val flightDS = flightsDF.as[Flight]

      println(s"Reading passenger data from: $passengerDataPath")
      val passengersDF = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(passengerDataPath)

      println("Passenger Data Schema:")
      passengersDF.printSchema()
      println("Passenger Data Sample:")
      passengersDF.show(5, false)

      val passengerDS = passengersDF.as[Passenger]

      def runAnalysis[T](name: String, analysis: => Dataset[T], outputPath: String): Unit = {
        try {
          println(s"Running $name...")
          val result = analysis
          writeToCSV(result, outputPath)
          println(s"$name completed. Results written to $outputPath")
        } catch {
          case e: Exception =>
            println(s"Error in $name: ${e.getMessage}")
            e.printStackTrace()
        }
      }

      runAnalysis("Flights per month", calculateFlightsPerMonth(flightDS), "output/q1_flights_per_month.csv")
      runAnalysis("Frequent flyers", findFrequentFlyers(flightDS, passengerDS, 100), "output/q2_frequent_flyers.csv")
      runAnalysis("Longest non-UK runs", findLongestNonUKRun(flightDS), "output/q3_longest_non_uk_runs.csv")
      runAnalysis("Passengers flown together", findPassengersFlownTogether(flightDS, 3), "output/q4_passengers_flown_together.csv")

      val from = Date.valueOf("2023-01-01")
      val to = Date.valueOf("2023-12-31")
      runAnalysis("Passengers flown together in date range", flownTogether(flightDS, 3, from, to), "output/q5_passengers_flown_together_in_range.csv")

      println("All analyses completed.")

    } catch {
      case e: Exception =>
        println(s"An error occurred in the main application: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}