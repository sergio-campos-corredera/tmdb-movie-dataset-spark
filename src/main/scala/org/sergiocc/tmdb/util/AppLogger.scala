package org.sergiocc.tmdb.util

import org.apache.log4j.Logger

trait AppLogger {
  @transient lazy val log: Logger = Logger.getLogger(classOf[AppLogger])
}
