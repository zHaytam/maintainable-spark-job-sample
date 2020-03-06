package agg

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Transformations {

  /** Joins the two dataframes while dropping unnecessary columns */
  def prepare(customersDf: DataFrame, transactionsDf: DataFrame): DataFrame = {
    customersDf.join(transactionsDf, Seq("userId"))
      .drop(col("id"))
  }

  /** Calculates, for each customer, the items he bought and the date of his last purchase.
   *
   *  Returns: DataFrame[name string, allItemsBought array<string>, lastPurchaseTime string]
   */
  def calculateCustomerBasedAggs(df: DataFrame): DataFrame = {
    df.groupBy("name")
      .agg(
        flatten(collect_list("items")).as("allItemsBought"),
        max("time").as("lastPurchaseTime")
      )
      .withColumn("allItemsBought", array_distinct(col("allItemsBought")))
  }

  /** Calculates, for each item, the number of times it was bought.
   *
   *  Returns: DataFrame[item string, count int]
   */
  def calculateItemBasedAggs(transactionsDf: DataFrame): DataFrame = {
    val itemsDf = transactionsDf.select("items")
      .withColumn("item", explode(col("items")))
      .drop("items")

    itemsDf.groupBy("item")
      .agg(count("*").as("count"))
      .orderBy(desc("count"))
  }

  /** Casts the "items" column into an array<string>. */
  def castItemsToArray(transactionsDf: DataFrame): DataFrame = {
    transactionsDf.withColumn("items", split(col("items"), ","))
  }

  /** Assigns the popularity of each item based on the number of times it was bought.<br />
   *  "high" if the item was bought 4 or more times, "low" otherwise.
   */
  def assignPopularity(df: DataFrame): DataFrame = {
    df.withColumn("popularity", when(col("count") >= 4, "high").otherwise("low"))
  }

}
