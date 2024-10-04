# Flight Data Analysis Project

## Table of Contents
- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Data](#data)
- [Analyses](#analyses)
- [Output](#output)
- [Testing](#testing)
- [Performance Considerations](#performance-considerations)
- [Known Limitations](#known-limitations)
- [Potential Performance Improvements](#potential-performance-improvements)
- [Future Enhancements](#future-enhancements)

## Overview
This project analyzes flight and passenger data to answer various questions about flight patterns and passenger behavior.

## Prerequisites
- Scala version 2.12.10
- Spark version 2.4.8
- JDK version 1.8
- SBT (Scala Build Tool)

## Installation
1. Clone this repository:

    ```sh
    git clone https://github.com/cativi/flight-data-analysis.git
    cd flightDataAnalysis
    ```

2. Ensure you have installed: Spark version 2.4.8, Scala version 2.12.10, and JDK version 1.8.

## Usage
To run the analysis:

```sh
sbt clean compile run
```

This will execute all analyses and output the results to CSV files in the output/ directory.

## Project Structure

flightDataAnalysis/
├── README.md
├── build.sbt
├── project/
│   └── build.properties
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   └── log4j.properties
│   │   └── scala/
│   │       ├── FlightDataAnalysisApp.scala
│   │       └── FlightStatistics.scala
│   └── test/
│       └── scala/
│           └── FlightStatisticsSpec.scala
├── data/
│   ├── flightData.csv
│   └── passengers.csv
└── output/
    ├── q1_flights_per_month.csv
    ├── q2_frequent_flyers.csv
    ├── q3_longest_non_uk_runs.csv
    ├── q4_passengers_flown_together.csv
    └── q5_passengers_flown_together_in_range.csv


## Data

The project uses two CSV files:

    - flightData.csv: Contains information about individual flights.
    - passengers.csv: Contains information about passengers.

## Analyses

The project performs the following analyses:

    - Total number of flights for each month.
    - Names of the 100 most frequent flyers.
    - Greatest number of countries a passenger has been in without being in the UK.
    - Passengers who have been on more than 3 flights together.
    - Passengers who have been on more than N flights together within a date range.

## Output

Results are written to CSV files in the output/ directory.

## Testing

To run the unit tests:

```sh
sbt test
```

## Performance Considerations

The Flight Data Analysis application is designed to process flight and passenger data efficiently using Apache Spark. Here are some key performance characteristics:

    - Data Processing: The application can handle CSV files containing flight and passenger data. It has been tested with files containing up to 100,000 and 15,000 rows, respectively.

    - Execution Environment: Currently configured for local execution (local[*]). For larger datasets, deploying on a Spark cluster is recommended.

    - Memory Usage: The application uses both memory and disk storage, particularly for the longest non-UK run analysis.

    - Serialization: Uses KryoSerializer for improved performance.

### Known Limitations

    - Schema inference is used, which may impact performance for very large datasets.
    - No explicit data partitioning strategy is implemented, which may affect performance with larger datasets.
    - Join operations in passenger analysis might become a bottleneck for extremely large datasets.

### Potential Performance Improvements

    - Data Partitioning: Implement partitioning based on frequently used columns (e.g., date) to improve query performance.
    - Join Optimization: Consider using broadcast joins for findPassengersFlownTogether and flownTogether functions if applicable.
    - Caching Strategy: Evaluate additional caching opportunities for frequently accessed datasets.
    - Schema Definition: Replace schema inference with explicit schema definitions for better performance and control.
    - Date Handling: Optimize date-based operations by pre-processing date columns into a more efficient format.

## Future Enhancements

    - Scalability: Adapt the application for execution on a distributed cluster for processing larger datasets.
    - Monitoring: Implement performance monitoring and logging for better insights and tuning.
    - Configurability: Add configuration options for key performance parameters (e.g., partition size, caching strategies).
    - Advanced Analytics: Explore possibilities for incorporating machine learning models for predictive analytics.
    - Real-time Processing: Consider adapting the system for real-time or streaming data processing.