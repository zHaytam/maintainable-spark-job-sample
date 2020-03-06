import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NormalJob extends Logging {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("SparkPageRank")
      .getOrCreate()

    // Load data
    val customersDf = spark
      .read
      .option("header", "true")
      .option("delimiter", ";")
      .csv("C:/sample_data/customers.csv")

    val transactionsDf = spark
      .read
      .option("header", "true")
      .option("delimiter", ";")
      .csv("C:/sample_data/transactions.csv")
      .withColumn("items", split(col("items"), ","))

    // Customer based aggregations
    val customersAndTransactionsDf = customersDf.join(transactionsDf, Seq("userId"))
      .drop("userId", "id", "joinDate")

    val cbaDf = customersAndTransactionsDf.groupBy("name")
      .agg(
        flatten(collect_list("items")).as("allItemsBought"),
        max("time").as("lastPurchaseTime")
      )

    // Item based aggregations
    val itemsDf = transactionsDf.select("items")
      .withColumn("item", explode(col("items")))
      .drop("items")

    var ibaDf = itemsDf.groupBy("item")
        .agg(count("*").as("count"))
        .orderBy(desc("count"))

    ibaDf = ibaDf.withColumn("popularity",
      when(col("count") >= 4, "high").otherwise("low"))

    // Save results
    cbaDf.coalesce(1).write.option("header", "true").parquet("C:/sample_data/cba")
    ibaDf.coalesce(1).write.option("header", "true").parquet("C:/sample_data/iba")
  }

}
