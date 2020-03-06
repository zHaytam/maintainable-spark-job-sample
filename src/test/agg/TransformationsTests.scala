package agg

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.types.DataTypes
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.{Matchers, WordSpec}

class TransformationsTests extends WordSpec
  with MockitoSugar
  with Matchers
  with ArgumentMatchersSugar
  with SparkSessionTestWrapper
  with DatasetComparer {

  import spark.implicits._

  "prepare" should {

    "join the two dataframes and drop the id column" in {
      // Arrange
      val customersDf = Seq((0, "name")).toDF("userId", "name")
      val transactionsDf = Seq((0, 0)).toDF("id", "userId")
      val expectedDf = Seq((0, "name")).toDF("userId", "name")

      // Act
      val actualDf = Transformations.prepare(customersDf, transactionsDf)

      // Assert
      assertSmallDatasetEquality(actualDf, expectedDf)
    }

    "throw a NullPointerException when customersDf is null" in {
      // Arrange
      val customersDf = null
      val transactionsDf = Seq((0, 0)).toDF("id", "userId")

      // Act & Assert
      assertThrows[NullPointerException] {
        Transformations.prepare(customersDf, transactionsDf)
      }
    }

  }

  "calculateCustomerBasedAggs" should {

    "aggregate by name" in {
      // Arrange
      val df = Seq(
        ("John", Array("a", "b"), "05/03/2020"),
        ("John", Array("c", "d"), "08/03/2020")
      ).toDF("name", "items", "time")
      val expectedDf = Seq(("John", Array("a", "b", "c", "d"), "08/03/2020"))
        .toDF("name", "allItemsBought", "lastPurchaseTime")

      // Act
      val actualDf = Transformations.calculateCustomerBasedAggs(df)

      // Assert
      assertSmallDatasetEquality(actualDf, expectedDf)
    }

    "remove duplicates in allItemsBought column" in {
      // Arrange
      val df = Seq(
        ("John", Array("a", "b"), "05/03/2020"),
        ("John", Array("b", "c", "d"), "08/03/2020")
      ).toDF("name", "items", "time")
      val expectedDf = Seq(("John", Array("a", "b", "c", "d"), "08/03/2020"))
        .toDF("name", "allItemsBought", "lastPurchaseTime")

      // Act
      val actualDf = Transformations.calculateCustomerBasedAggs(df)

      // Assert
      assertSmallDatasetEquality(actualDf, expectedDf)
    }

  }

  "calculateItemBasedAggs" should {

    "aggregate by item and calculate count per item (ordered by count)" in {
      // Arrange
      val transactionsDf = Seq((Array("a", "b", "c")), (Array("c", "d"))).toDF("items")
      val expectedDf = Seq(("c", 2L), ("a", 1L), ("b", 1L), ("d", 1L)).toDF("item", "count")

      // Act
      val actualDf = Transformations.calculateItemBasedAggs(transactionsDf)

      // Assert
      assertSmallDatasetEquality(actualDf, expectedDf)
    }

  }

  "castItemsToArray" should {

    "cast items column into array<string>" in {
      // Arrange
      val transactionsDf = Seq("a, b, c").toDF("items")

      // Act
      val actualDf = Transformations.castItemsToArray(transactionsDf)

      // Assert
      assert(actualDf.schema.fields(0).dataType == DataTypes.createArrayType(DataTypes.StringType))
    }

  }

  "assignPopularity" should {

    "assign popularity correctly" in {
      // Arrange
      val df = Seq((2), (4), (5)).toDF("count")
      val expectedDf = Seq((2, "low"), (4, "high"), (5, "high")).toDF("count", "popularity")

      // Act
      val actualDf = Transformations.assignPopularity(df)

      // Assign
      assertSmallDatasetEquality(actualDf, expectedDf, ignoreNullable = true)
    }

  }

}
