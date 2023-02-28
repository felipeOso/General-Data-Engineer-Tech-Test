package com.general.data.engineer.tech.test.services

import com.general.data.engineer.tech.test.config.ConfigTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ProcessingServiceTest extends AnyWordSpec with Matchers with ConfigTest {

  val service = new ProcessingService(sparkSession, config)
  "Test ProcessingServiceTest" should {

    "Processing dataframe use case 1" when {
      "Service calling function transform" in {
        val keyExpected = Row(1)
        val valueExpected = Row(1)

        val dataset = service.transform(createDataframeUseCase1)

        dataset.select(col("key")).collect()(0) mustBe keyExpected
        dataset.select(col("count")).collect()(0) mustBe valueExpected
      }
    }
    "Processing dataframe use case 2" when {
      "Service calling function transform." in {
        val keyExpected = Row(2)
        val valueExpected = Row(3)

        val dataset = service.transform(createDataframeUseCase2)

        dataset.select(col("key")).collect()(0) mustBe keyExpected
        dataset.select(col("count")).collect()(0) mustBe valueExpected
      }
    }

    "Processing dataframe use case 3" when {
      "Service calling function transform." in {
        val keyExpected = Row(1)
        val valueExpected = Row(3)

        val dataset = service.transform(createDataframeUseCase3)

        dataset.select(col("key")).collect().contains(Row(2)) mustBe false
        dataset.select(col("key")).collect().contains(Row(5)) mustBe false
        dataset.select(col("key")).collect().contains(keyExpected) mustBe true
        dataset.select(col("count")).collect().contains(valueExpected) mustBe true
      }
    }

    /*     "Extract Information" when {
           "Calling the extract method, get information, it must return a  dataframe." in {
             val dataset = service.extract()
             dataset.take(3) mustBe createDataframe.take(3)
           }
         }*/

  }
}
