/**
 * Spark 2 Recruitment Challenge
 * Xpand it
 *
 * Utils file contains all da functions responsible for fulfilling
 * what was asked on each part of the challenge.
 *
 * Author: Afonso Machado
 */
package org.example.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType}

object Utils {

  /**
   * Average of the column Sentiment_Polarity grouped by App name
   *
   * @param userReviews googleplaystore_user_reviews.csv
   * @return df_1
   */
  def partOne(userReviews: DataFrame): DataFrame = userReviews
    .groupBy("App")
    .agg(avg(userReviews("Sentiment_Polarity")).as("Average_Sentiment_Polarity"))
    .withColumn("Average_Sentiment_Polarity", col("Average_Sentiment_Polarity").cast(DoubleType))
    .na.fill(0)

  /**
   * Apps with a "Rating" greater or equal to 4.0 sorted in descending order.
   * Save that Dataframe as a CSV (delimiter: "§") named "best_apps.csv"
   *
   * @param user_reviws googleplaystore.csv
   * @return df_2
   */
  def partTwo(user_reviws: DataFrame): DataFrame = {
    user_reviws
      .withColumn("Rating", user_reviws("Rating").cast(DoubleType))
      .filter(user_reviws("Rating") >= 4.0 && !isnan(user_reviws("Rating")))
      .orderBy(desc("Rating"))
  }

  /**
   * App unique value.
   * In case of App duplicates, the column "Categories" of the resulting row should contain an array.
   * In case of App duplicates for all columns except categories, the remaining columns should have
   * the same values as the ones on the row with the maximum number of reviews.
   *
   * @param apps googleplaystore.csv
   * @return df_3
   */
  def partThree(apps: DataFrame): DataFrame = {

    val df_3 = apps
      .withColumnRenamed("Content Rating", "Content_Rating")
      .withColumnRenamed("Last Updated", "Last_Updated")
      .withColumnRenamed("Current Ver", "Current_Version")
      .withColumnRenamed("Android Ver", "Minimum_Android_Version")
      .withColumn("Rating", regexp_replace(col("Rating"), "[NaN]", "null"))
      .withColumn("Rating", col("Rating").cast(DoubleType))
      .withColumn("Last_Updated",
        to_date(col("Last_updated"), "MMMM dd, yyyy")
      )
      .withColumn(
        "Last_updated",
        date_format(col("Last_updated"), "yyyy-MM-dd hh:mm:ss")
      )
      .withColumn(
        "Minimum_Android_Version",
        regexp_replace(col("Minimum_Android_Version"), " and up", "")
      )
      .withColumn("Reviews", col("Reviews").cast(LongType))
      .withColumn("Price", regexp_replace(col("Price"), "[$]", ""))
      .withColumn("Price", col("Price").cast(DoubleType))
      .withColumn("Price", col("Price") * 0.9)
    //.withColumn("Price", concat(col("Price"), lit('€'))) -> € symbol does not display correctly

    val dfAppsUniqueValue = apps
      .groupBy("App")
      .agg(
        collect_set("Category").alias("Categories"),
        max(col("Reviews")).as("Reviews")
      )
      .select("App", "Categories", "Reviews")

    df_3
      .join(
        dfAppsUniqueValue,
        df_3("App") === dfAppsUniqueValue("App") && df_3("Reviews") === dfAppsUniqueValue("Reviews"),
        "Right"
      )
      .withColumn("Genres", split(col("Genres"), ";").as("Genres"))
      .withColumn("Size",
        when(
          !df_3("Size").contains("Varies with device") && df_3("Size").contains("k"),
          regexp_replace(df_3("Size"), "k", "").cast(DoubleType) * 1024
        )
          .when(
            !df_3("Size").contains("Varies with device") && df_3("Size").contains("M"),
            regexp_replace(df_3("Size"), "M", "").cast(DoubleType)
          )
          .otherwise(null)
      )
      .select(
        df_3("App"),
        dfAppsUniqueValue("Categories"),
        df_3("Rating"),
        df_3("Reviews"),
        col("Size"),
        df_3("Installs"),
        df_3("Type"),
        df_3("Price"),
        df_3("Content_Rating"),
        col("Genres"),
        df_3("Last_Updated"),
        df_3("Current_Version"),
        df_3("Minimum_Android_Version")
      ).where(df_3("App") === dfAppsUniqueValue("App"))
  }

  /**
   * Given the Dataframes produced by Exercise 1 and 3,
   * produce a Dataframe with all its information plus its 'Average_Sentiment_Polarity' calculated in Exercise 1
   *
   * @param dataFrame         Result of exercise 3
   * @param dfAveragePolarity Result of exercise 1
   * @return df_3joined
   */
  def partFour(dataFrame: DataFrame, dfAveragePolarity: DataFrame): DataFrame = {
    dataFrame
      .join(
        dfAveragePolarity,
        dataFrame("App") === dfAveragePolarity("App"),
        "right"
      )
      .select(
        dataFrame("App"),
        dataFrame("Categories"),
        dataFrame("Rating"),
        dataFrame("Reviews"),
        dataFrame("Size"),
        dataFrame("Installs"),
        dataFrame("Type"),
        dataFrame("Price"),
        dataFrame("Content_Rating"),
        dataFrame("Genres"),
        dataFrame("Last_Updated"),
        dataFrame("Current_Version"),
        dataFrame("Minimum_Android_Version"),
        dfAveragePolarity("Average_Sentiment_Polarity")
      )
      .where(dataFrame("App") === dfAveragePolarity("App"))
  }

  /**
   * Using df_3 create a new Dataframe containing the number of applications,
   * the average rating and the average sentiment polarity by genre.
   *
   * @param dataFrame df_3
   * @return df_4
   */
  def partFive(dataFrame: DataFrame): DataFrame = {

    val genresExploded = dataFrame
      .withColumn(
        "Genre",
        explode(dataFrame.col("Genres"))
      )
      .select(
        dataFrame("App"),
        col("Genre"),
        dataFrame("Rating"),
        dataFrame("Average_Sentiment_Polarity")
      )

    genresExploded
      .groupBy("Genre")
      .agg(
        count(col("App")).as("Count"),
        avg("Rating").as("Average_Rating"),
        avg("Average_Sentiment_Polarity").as("Average_Sentiment_Polarity")
      )
      .select(
        genresExploded("Genre"),
        col("Count"),
        col("Average_Rating"),
        col("Average_Sentiment_Polarity")
      )
  }

}