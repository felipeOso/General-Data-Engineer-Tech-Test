package com.general.data.engineer.tech.test.services

import com.general.data.engineer.tech.test.config.Config
import com.general.data.engineer.tech.test.utilities.Logger.logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._

final class ProcessingService(sparkSession: SparkSession, config: Config){
  def extract(): DataFrame = {
    logger.info("Extract Data")
    sparkSession.read
      .option("header", "true") // files contain headers
      .option("inferSchema", "true") // infer schema from data
      // .option("delimiter", "\t") // handle both CSV and TSV files
      .csv(config.readPath.value)
  }

  def transform(frame: DataFrame) = {
    logger.info("Transform Data")

    // Replace empty strings with 0
    val data = frame.na.fill(0)
    // Rename columns to "key" and "value"
    val renamedData = data
      .withColumnRenamed(data.columns(0), "key")
      .withColumnRenamed(data.columns(1), "value")

    renamedData
      .groupBy(renamedData.columns.map(col): _*)
      .count()
      .filter(col("count") % 2 === 1)
      .dropDuplicates(Seq("key")).orderBy("key")
  }

  def load(dataset: Dataset[Row]) = {
    logger.info(s"Data: ${dataset.show(5)}")
    dataset.write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(config.writePath.value)
  }
}

object ProcessingService {
  def apply(sparkSession: SparkSession, config:Config): ProcessingService = new ProcessingService(sparkSession, config)
}


