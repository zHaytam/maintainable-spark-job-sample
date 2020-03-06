package agg

import org.apache.spark.internal.Logging

class MaintainableJob(ioHandler: IOHandler) extends Logging {

  def run(): Unit = {
    // Load & prepare data
    val customersDf = ioHandler.loadCsv("C:/sample_data/customers.csv", delimiter = ";")
    val transactionsDf = ioHandler.loadCsv("C:/sample_data/transactions.csv", delimiter = ";")
      .transform(Transformations.castItemsToArray)
    val df = Transformations.prepare(customersDf, transactionsDf)

    // Aggregations
    val cbaDf = Transformations.calculateCustomerBasedAggs(df)
    val ibaDf = Transformations.calculateItemBasedAggs(df).transform(Transformations.assignPopularity)

    // Save results
    ioHandler.saveParquet(cbaDf, "C:/sample_data/cba")
    ioHandler.saveParquet(ibaDf, "C:/sample_data/iba")
  }

}
