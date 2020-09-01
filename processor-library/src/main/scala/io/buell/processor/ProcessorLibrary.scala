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
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus

import scala.collection.mutable.ListBuffer

class ProcessorLibrary {
  private val logger = LoggerFactory.getLogger(classOf[ProcessorLibrary])

  private val key = System.getProperty("coinbase-pro.key")
  private val secretKey = System.getProperty("coinbase-pro.secretKey")
  private val passphrase = System.getProperty("coinbase-pro.passphrase")

  val spark: SparkSession = SparkSession
    .builder
    .appName("ProcessorLibrary")
    .config("spark.master", "local")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext
  val sql: SQLContext = spark.sqlContext

  private val mapper = new ObjectMapper

  private var df: DataFrame = null
  private val url = System.getProperty("coinbase-pro.url")
  private var epoch: Long = 1483228800
  private val candles = 300
  private val granularity = 60

  @throws[IOException]
  @throws[URISyntaxException]
  @throws[ParseException]
  @throws[InterruptedException]
  def fetch(pair: String): Unit = {
    val client = HttpClients.createDefault
    while ( {
      epoch < new Date().toInstant.getEpochSecond
    }) {

      ///// Compute new epoch
      epoch = epoch + candles * granularity

      ///// Build URI for request
      val builder = new URIBuilder("https://" + url)
      builder.setPath("/products/" + pair + "/candles")
        .setParameter("granularity", String.valueOf(granularity))
        .setParameter("start", Utils.getDateForISO8601(epoch))
        .setParameter("end", Utils.getDateForISO8601(epoch))

      val getProductCandlesRequest = new HttpGet(builder.build)
      val getProductCandlesResponse = client.execute(getProductCandlesRequest)

      if (HttpStatus.resolve(getProductCandlesResponse.getStatusLine.getStatusCode).is2xxSuccessful) {
        val getProductCandlesResponseNode = mapper.readTree(EntityUtils.toString(getProductCandlesResponse.getEntity))
        read(getProductCandlesResponseNode, epoch)
        logger.info("End: " + epoch + ", " + Utils.getDateForISO8601(epoch))
      }
      else {
        logger.error("Error processing request " + String.valueOf(getProductCandlesResponse.getStatusLine.getStatusCode))
        throw new Exception("Error processing request: " + getProductCandlesResponse)
      }

      ///// Sleep .5s due to request throttling
      ///// Probably could be optimized but w/e
      Thread.sleep(500)
    }
  }

  private var beginning: Long = 0
  private var previousTicks: ListBuffer[tick] = new ListBuffer[tick]

  def read(result: JsonNode, epoch: Long): Unit = {

    val elements = result.elements()
    var newTicks = new ListBuffer[tick]

    while (elements.hasNext) {
      val element = elements.next()
      newTicks += tick(
        element.get(0).longValue,
        element.get(1).floatValue,
        element.get(2).floatValue,
        element.get(3).floatValue,
        element.get(4).floatValue,
        element.get(5).floatValue
      )
    }

    newTicks = newTicks.sorted

    var ticks: ListBuffer[tick] = new ListBuffer[tick]

    if (previousTicks.nonEmpty) {
      for (ListBuffer(lower, upper) <- previousTicks.sliding(2)) {
        ticks += lower
        for (i <- (lower.epoch + 60) until upper.epoch by 60) {
          ticks += tick(i, lower.low, lower.high, lower.open, lower.close, 0)
        }
      }
    }

    ticks = ticks.sorted

    if (ticks.nonEmpty) {
      val lastTick = ticks.last
      for (i <- (lastTick.epoch + 60) until newTicks.head.epoch by 60) {
        ticks += tick(i, lastTick.low, lastTick.high, lastTick.open, lastTick.close, 0)
      }
    }

    ticks = ticks.sorted

    previousTicks = newTicks
    previousTicks = previousTicks.sorted

    val iteration = spark.createDataFrame(ticks)

    if (df == null) {
      beginning = epoch
      df = iteration
    }
    else {
      df = df.union(iteration)
    }

    ///// Limit DF to an arbitrary size of 24000 then write and clean
    ///// Filename contains the epochs of the first and last records
    ///// TODO: take df size as req param or env var?
    if (24000 <= df.count) {
      beginning = df.orderBy("epoch").first().getLong(0)

      df.orderBy("epoch")
        .coalesce(1)
        .write
        .option("header", value = true)
        .csv("ticks/csv/%s_%s" format (beginning, ticks.last.epoch))

      df.orderBy("epoch")
        .coalesce(1)
        .write
        .option("header", value = true)
        .parquet("ticks/parquet/%s_%s" format (beginning, ticks.last.epoch) )

      df = null
    }
  }

  case class tick(epoch: Long, low: Float, high: Float, open: Float, close: Float, volume: Float) extends Ordered[tick] {
    def compare(that: tick): Int = (this.epoch) compare (that.epoch)
  }
}
