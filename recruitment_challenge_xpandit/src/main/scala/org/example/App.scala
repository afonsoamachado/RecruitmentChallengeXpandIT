/**
 * Spark 2 Recruitment Challenge
 * Xpand it
 *
 * Main application.
 *
 * Author: Afonso Machado
 */
package org.example


import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.example.utils.Utils

object App extends App {

  //Unified entry point of a spark application
  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("RecruitmentChallenge")
    .getOrCreate()

  val user_reviews = spark
    .read
    .option("delimiter", ",")
    .option("header", "true")
    .csv("src/main/scala/org/example/samples/googleplaystore_user_reviews.csv")

  val apps = spark
    .read
    .option("delimiter", ",")
    .option("header", "true")
    .csv("src/main/scala/org/example/samples/googleplaystore.csv")

  //Part 1
  val df_1: DataFrame = Utils.partOne(user_reviews)
  df_1.show()

  //Part 2
  val df2 = Utils
    .partTwo(apps)
  //Save that Dataframe as a CSV (delimiter: "§") named "best_apps.csv".
  df2
    .write
    .mode(SaveMode.Overwrite)
    .option("header", value = true)
    .option("delimiter", "§")
    .format("csv")
    .save("src/main/scala/org/example/results/best_apps.csv")

  //Part 3
  val df_3 = Utils.partThree(apps)
  df_3.show()

  //Part 4
  val df_3joined = Utils.partFour(df_3, df_1)
  //Save the final Dataframe as a parquet file with gzip compression with the name "googleplaystore_cleaned".
  df_3joined
    .write
    .mode(SaveMode.Overwrite)
    .option("header", value = true)
    .parquet("src/main/scala/org/example/results/googleplaystore_cleaned")


  //Part 5
  val df_4 = Utils.partFive(df_3joined)
  //Save it as a parquet file with gzip compression with the name "googleplaystore_metrics".
  df_4
    .write
    .mode(SaveMode.Overwrite)
    .option("header", value = true)
    .parquet("src/main/scala/org/example/results/googleplaystore_metrics")
}

