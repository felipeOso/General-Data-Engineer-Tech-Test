package com.general.data.engineer.tech.test.utilities

import org.apache.log4j.Logger

/**
 * Logger for App.
 **/
object Logger extends Serializable {

  lazy val logger:Logger= org.apache.log4j.Logger.getLogger(getClass.getName)
}
