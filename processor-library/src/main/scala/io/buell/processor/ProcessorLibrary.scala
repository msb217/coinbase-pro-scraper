package io.buell.processor

import java.io.IOException
import java.net.URISyntaxException
import java.text.ParseException
import java.util.Date

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus

import scala.collection.mutable.ListBuffer

class ProcessorLibrary {
  private val key = System.getProperty("coinbase-pro.key")
  private val secretKey = System.getProperty("coinbase-pro.secretKey")
  private val passphrase = System.getProperty("coinbase-pro.passphrase")
  private val url = System.getProperty("coinbase-pro.url")
  private val logger = LoggerFactory.getLogger(classOf[ProcessorLibrary])

  // 2016-01-01T00:00:00Z
  private var epoch: Long = 1483228800
  private val candles = 300
  private val granularity = 60

  val spark = SparkSession
    .builder
    .appName("ProcessorLibrary")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext
  val sql = spark.sqlContext

  private var df: DataFrame = null

  @throws[IOException]
  @throws[URISyntaxException]
  @throws[ParseException]
  @throws[InterruptedException]
  def fetch(pair: String): Unit = {
    val client = HttpClients.createDefault
    while ( {
      epoch < new Date().toInstant.getEpochSecond
    }) {
      var builder = new URIBuilder("https://" + url)
      builder.setPath("/products/" + pair + "/candles")
      builder.setParameter("granularity", String.valueOf(granularity))
      builder.setParameter("start", Utils.getDateForISO8601(epoch))
      epoch = epoch + candles * granularity
      builder.setParameter("end", Utils.getDateForISO8601(epoch))
      val get = new HttpGet(builder.build)
      val response = client.execute(get)
      val mapper = new ObjectMapper
      if (HttpStatus.resolve(response.getStatusLine.getStatusCode).is2xxSuccessful) {
        var responseNode = mapper.readTree(EntityUtils.toString(response.getEntity))
        //logger.info(EntityUtils.toString(response.getEntity))
        read(responseNode, epoch)
        logger.info("End: " + epoch + ", " + Utils.getDateForISO8601(epoch))
      }
      else {
        logger.error("Error processing request " + String.valueOf(response.getStatusLine.getStatusCode))
        throw new Exception("Error processing request: "+response)
      }
      Thread.sleep(500)
    }
  }

  private var previousElement: JsonNode = null
  private var beginning: Long = 0
  private var previousTicks: ListBuffer[tick] = new ListBuffer[tick]

  def read(result: JsonNode, epoch: Long): Unit = {

    var elements = result.elements()

    var newTicks = new ListBuffer[tick]

    while(elements.hasNext == true) {
      var element = elements.next()
      newTicks += tick(element.get(0).longValue, element.get(1).floatValue, element.get(2).floatValue, element.get(3).floatValue, element.get(4).floatValue, element.get(5).floatValue())
    }

    newTicks = newTicks.sorted

    var ticks: ListBuffer[tick] = new ListBuffer[tick]

    if(!previousTicks.isEmpty) {
      for (ListBuffer(lower, upper) <- previousTicks.sliding(2)) {
        ticks += lower
        for (i <- (lower.epoch + 60) until upper.epoch by 60) {
          ticks += tick(i, lower.low, lower.high, lower.open, lower.close, 0)
        }
      }
    }

    ticks = ticks.sorted

    if(!ticks.isEmpty) {
      var lastTick = ticks(ticks.length - 1)
      for (i <- (lastTick.epoch + 60) until newTicks(0).epoch by 60) {
        ticks += tick(i, lastTick.low, lastTick.high, lastTick.open, lastTick.close, 0)
      }
    }

    ticks = ticks.sorted

    previousTicks = newTicks

    previousTicks = previousTicks.sorted

    var iteration = spark.createDataFrame(ticks)

    if (df == null) {
      beginning = epoch
      df = iteration
    }
    else {
      df = df.union(iteration)
    }

    if (24000 <= df.count) {
      beginning = df.orderBy("epoch").first().getLong(0)
      df.orderBy("epoch").coalesce(1).write.csv("ticks/csv/" + beginning +"_"+ticks(ticks.length - 1).epoch)
      df.orderBy("epoch").coalesce(1).write.parquet("ticks/parquet/" +beginning+"_"+ ticks(ticks.length - 1).epoch)
      df = null
    }
  }

  case class tick(epoch: Long, low: Float, high: Float, open: Float, close: Float, volume: Float) extends Ordered[tick] {
    def compare(that: tick): Int = (this.epoch) compare (that.epoch)
  }
}
