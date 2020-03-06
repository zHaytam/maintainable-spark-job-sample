package agg


import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Maintainable Job")
      .getOrCreate()

    val ioHandler = new IOHandler(spark)
    val job = new MaintainableJob(ioHandler)
    job.run()

    spark.stop()
  }

}
