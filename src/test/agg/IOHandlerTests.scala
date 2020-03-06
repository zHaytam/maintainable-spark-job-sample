package agg

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter, Row, SparkSession}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.{Matchers, WordSpec}

class IOHandlerTests extends WordSpec with MockitoSugar with Matchers with ArgumentMatchersSugar {

  "loadCsv" should {

    "call spark.read.csv with correct option values" in {
      // Arrange
      val spark = mock[SparkSession]
      val mockReader = mock[DataFrameReader]
      val ioHandler = new IOHandler(spark)

      when(spark.read) thenReturn mockReader
      when(mockReader.option("header", true)) thenReturn mockReader
      when(mockReader.option("delimiter", ",")) thenReturn mockReader

      // Act
      ioHandler.loadCsv("filename")

      // Assert
      verify(mockReader).csv("filename")
    }

  }

  "saveParquet" should {

    "call write.parquet with correct options" in {

      // Arrange
      val spark = mock[SparkSession]
      val mockWriter = mock[DataFrameWriter[Row]]
      val mockDf = mock[DataFrame]
      val ioHandler = new IOHandler(spark)

      when(mockDf.coalesce(1)) thenReturn mockDf
      when(mockDf.write) thenReturn mockWriter
      when(mockWriter.option("header", true)) thenReturn mockWriter

      // Act
      ioHandler.saveParquet(mockDf, "filename")

      // Assert
      verify(mockWriter).parquet("filename")
    }

  }

}
