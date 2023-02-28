package com.general.data.engineer.tech.test.config

import org.apache.spark.sql.SparkSession

object Connection {

  /**
   * This method is responsible for starting the spark session and initializing the connection with amazon if it exists.
   */
  def start(): SparkSession = {
    SparkSession.builder()
      .appName(Config.spark.appName)
      .master(Config.spark.master)
      //.config(Config.amazonConfig.readAuth.authType.name, Config.amazon.readAuth.authType.value)
      //.config(Config.amazonConfig.readAuth.clientId.name, Config.amazon.readAuth.clientId.value)
      .getOrCreate()
  }
}
