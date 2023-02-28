package com.general.data.engineer.tech.test.config

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import pureconfig.ConfigSource
import pureconfig.generic.auto._
trait ConfigTest extends AnyWordSpec with BeforeAndAfterAll {

  val config: Config = ConfigSource.default.loadOrThrow[Config]

  val sparkSession: SparkSession =
    SparkSession
      .builder()
      .appName(config.spark.appName)
      .master(config.spark.master)
      .getOrCreate()

/*  override protected def beforeAll(): Unit = {
    writeCSVInputTest()
  }*/

  override protected def afterAll() = {
    sparkSession.stop()
  }

  def writeCSVInputTest: () => Unit = {
    () =>
      createDataframeUseCase1
        .write
        .option("header", true)
        .mode(SaveMode.Overwrite)
        .csv(config.writePath.value)
  }

  // Define schema of dataframe
  val schema = StructType(
    StructField("key", IntegerType, nullable = false) ::
      StructField("value", IntegerType, nullable = false) :: Nil
  )

  // Create RDD
  def getRDD(rows:Seq[Row]) = {
    sparkSession.sparkContext.parallelize(rows)
  }

  def createDataframeUseCase1 = {
    // Create sequence of rows
    val rows = Seq(
      Row(1, 2),
      Row(1, 3),
      Row(1, 3)
    )
    // Create Dataframe
    sparkSession.createDataFrame(getRDD(rows), schema)
  }

  def createDataframeUseCase2 = {
    // Create sequence of rows
    val rows = Seq(
      Row(2, 4),
      Row(2, 4),
      Row(2, 4)
    )
    // Create Dataframe
    sparkSession.createDataFrame(getRDD(rows), schema)
  }

  def createDataframeUseCase3 = {
    // Create sequence of rows
    val rows = Seq(
      Row(2, 4),
      Row(2, 4),
      Row(1, 3),
      Row(1, 3),
      Row(1, 3),
      Row(5, 3),
      Row(5, 3),
    )
    // Create Dataframe
    sparkSession.createDataFrame(getRDD(rows), schema)
  }

}
