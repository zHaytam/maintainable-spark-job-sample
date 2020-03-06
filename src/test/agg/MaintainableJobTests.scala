package agg

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.DataFrame
import org.mockito.captor.ArgCaptor
import org.mockito.{ArgumentCaptor, ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.{Matchers, WordSpec}

class MaintainableJobTests extends WordSpec
  with MockitoSugar
  with Matchers
  with ArgumentMatchersSugar
  with SparkSessionTestWrapper
  with DatasetComparer {

  import spark.implicits._

  "run" should {

    "transform data and save results as parquet" in {
      // Arrange
      val customersDf = Seq(
        (1, "Sam", "01/01/2020"),
        (2, "Samantha", "02/01/2020")
      ).toDF("userId", "name", "joinDate")
      val transactionsDf = Seq(
        (1, 1, 20, "a,b,c", "01/01/2020"),
        (2, 2, 10, "c,e,f", "02/01/2020"),
        (3, 1, 39, "b,c,c,d", "03/01/2020")
      ).toDF("id", "userId", "total", "items", "time")
      val cbaDf = Seq(
        ("Sam", Array("a", "b", "c", "d"), "03/01/2020"),
        ("Samantha", Array("c", "e", "f"), "02/01/2020")
      ).toDF("name", "allItemsBought", "lastPurchaseTime")
      val ibaDf = Seq(
        ("c", 4L, "high"),
        ("b", 2L, "low"),
        ("a", 1L, "low"),
        ("e", 1L, "low"),
        ("f", 1L, "low"),
        ("d", 1L, "low")
      ).toDF("item", "count", "popularity")

      val ioHandler = mock[IOHandler]
      val cbaCaptor = ArgCaptor[DataFrame]
      val ibaCaptor = ArgCaptor[DataFrame]

      when(ioHandler.loadCsv("C:/sample_data/customers.csv", delimiter = ";")) thenReturn customersDf
      when(ioHandler.loadCsv("C:/sample_data/transactions.csv", delimiter = ";")) thenReturn transactionsDf

      // Act
      new MaintainableJob(ioHandler).run()

      // Assert
      verify(ioHandler).saveParquet(cbaCaptor, eqTo("C:/sample_data/cba"), eqTo(true))
      verify(ioHandler).saveParquet(ibaCaptor, eqTo("C:/sample_data/iba"), eqTo(true))
      assertSmallDatasetEquality(cbaCaptor.value, cbaDf, ignoreNullable = true)
      assertSmallDatasetEquality(ibaCaptor.value, ibaDf, ignoreNullable = true)
    }

  }

}
