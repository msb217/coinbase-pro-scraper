package io.buell.processor

import java.text.DateFormat
import java.text.ParseException
import java.text.SimpleDateFormat
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.TimeZone

import ch.qos.logback.classic.{Level, Logger}


object Utils {
  def setLoggingLevel(level: Level): Unit = {
    val root = org.slf4j.LoggerFactory.getLogger("root").asInstanceOf[Logger]
    root.setLevel(level)
  }

  /**
    * Return an ISO 8601 combined date and time string for specified date/time
    *
    * @param date
    * Date
    * @return String with format "yyyy-MM-dd'T'HH:mm:ss'Z'"
    */
  def getISO8601ForDate(date: String): String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
    dateFormat.format(date)
  }

  /**
    * Returns a date/time for an ISO 8601 string
    *
    * @param iso
    * Date
    * @return String with format in ISO8601
    */
  @throws[ParseException]
  def getDateForISO8601(epoch: Long): String = {
    val i = Instant.ofEpochSecond(epoch)
    val z = ZonedDateTime.ofInstant(i, ZoneOffset.UTC)
    z.toString
  }

  @throws[ParseException]
  def getDateForISO8601(epoch: String): String = getDateForISO8601(epoch.toLong)
}
