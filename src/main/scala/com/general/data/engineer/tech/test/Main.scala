package com.general.data.engineer.tech.test

import com.general.data.engineer.tech.test.config.{Config, Connection}
import com.general.data.engineer.tech.test.services.ProcessingService
import com.general.data.engineer.tech.test.utilities.Logger.logger

object Main {
  def main(args: Array[String]): Unit = {

    logger.info("Start execution Spark...")

    val sparkSession = Connection.start()
    val config = Config(Config.spark, Config.readPath, Config.writePath)

    def run() = {
      val service  = ProcessingService(sparkSession, config)
      service.load(service.transform(service.extract()))
    }
    run()
    logger.info("Assessment executed Successfully")
    sparkSession.stop()
  }
}
