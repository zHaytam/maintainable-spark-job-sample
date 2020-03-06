package agg

import org.apache.spark.sql.{DataFrame, SparkSession}

class IOHandler(spark: SparkSession) {

  def loadCsv(filename: String, header: Boolean = true, delimiter: String = ","): DataFrame = {
    spark.read
      .option("header", header)
      .option("delimiter", delimiter)
      .csv(filename)
  }

  def saveParquet(df: DataFrame, filename: String, header: Boolean = true): Unit = {
    df.coalesce(1)
      .write
      .option("header", header)
      .parquet(filename)
  }

}
