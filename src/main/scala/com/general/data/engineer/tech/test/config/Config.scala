package com.general.data.engineer.tech.test.config

import pureconfig.ConfigSource
import pureconfig.generic.auto._

final case class Config private(spark: SparkConfig, readPath: ReadPath, writePath: WritePath)
final case class SparkConfig private(appName: String, master: String)
final case class ReadPath private(value: String)
final case class WritePath private(value: String)


/** Example for mapping configuration for amazon S3
 * final case class AmazonConfig private (readAuth:AmazonAuth, writeAuth: AmazonAuth)
 * final case class AmazonAuth private(authType:AuthType)
 * final case class AuthType private (name:String, value:String)
 */
object Config {
  private val config: Config = ConfigSource.default.loadOrThrow[Config]
  val spark: SparkConfig = config.spark
  val readPath: ReadPath = config.readPath
  val writePath: WritePath = config.writePath
}