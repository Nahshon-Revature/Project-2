package TwitterAPI

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.nio.file.{Files, Paths}
import java.util.concurrent._

import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.spark.sql.{SparkSession, functions}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.StdIn

object Runner {
  def main(args: Array[String]): Unit = {
    // Goal: automate trend get request every 15 min for 1 hour, 12 hours, 24 hours (as long as
    // the program runs). https://api.twitter.com/1.1/trends/place.json?id=*woeid here*
    //SUCCESS!!

    //Ask user for woeid to use as query input, and frequency + interval for repeated queries.
    userinputs

    //Using Bearer Token to authenticate api requests
    //Make this variable name ("TWITTER_BEARER_TOKEN") match yours
    val bearer_token = System.getenv("TWITTER_BEARER_TOKEN")
    println(s"Authenticating with Bearer Token: ${bearer_token}")

    //Set up an executor and runnable to repeat query.
    //Send get request to API, then convert response to parsable json.
    //Automatically cancels at end of interval.
    val exec = new ScheduledThreadPoolExecutor(1)
    val goodTask = new Runnable {
      def run() =  {
        Converter.convert(trendsToLine(bearer_token,location))
      }
    }
    val f = exec.scheduleAtFixedRate(goodTask, 0, frequency, TimeUnit.MILLISECONDS)
    exec.schedule( new Runnable {
      override def run(): Unit = f.cancel(false)
    }, interval, TimeUnit.MILLISECONDS)


    //creating spark session
//    val sesh = SparkSession.builder()
//      .appName("Twitter API Program")
//      .master("local[4]")
//      .getOrCreate()
//    import sesh.implicits._
//    sesh.sparkContext.setLogLevel("WARN")

    //static dataframe (maybe change to dataset?)
    //val staticDF = sesh.read.json("Geo-trend-results")

  }

  //global scope input param defaults.
  var location = 2459115
  var interval = 3600000
  var frequency = 900000

  def userinputs = {
    val userwoeid = StdIn.readLine("Please enter a WOEID. By default, New York City (Capitol of the Galaxy) is used." +
      "Enter '1' to pull global trends.\nWOEID: ")

    if (userwoeid == "") {
      println("No woeid entered, using New York.")
    } else if (userwoeid.forall(Character.isDigit)) {
      location = userwoeid.toInt
    } else {
      println("That may not be a valid woeid; Using New York.")
    }

    val userinterval = StdIn.readLine("How long should we pull trends for? Please enter an interval in hours.\nInterval: ")

    if (userinterval == "") {
      println("No interval entered, defaulting to 1 hour.")
    } else if (userinterval.forall(Character.isDigit)) {
      interval = (userinterval.toInt * 3600000)
    } else {
      println("That may not be a valid number; Defaulting to 1 hour.")
    }

    val userfreq = StdIn.readLine("How frequently should we pull trends? Please enter a frequency in minutes.\nFrequency: ")

    if (userfreq == "") {
      println("No frequency entered, defaulting to 15 minutes.")
    } else if (userfreq.forall(Character.isDigit)) {
      frequency = (userfreq.toInt * 60000)
    } else {
      println("That may not be a valid number; Defaulting to 15 minutes.")
    }
  }

  //API http request. Gets json-like response and reads in as a string line.
  def trendsToLine (authToken: String, woeid: Int, dirName: String="trendsbywoeid"): String = {
    val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build
    val uriBuilder = new URIBuilder(s"https://api.twitter.com/1.1/trends/place.json?id=${woeid}")
    val httpGet = new HttpGet(uriBuilder.build)
    httpGet.setHeader("Authorization", String.format("Bearer %s", authToken))
    val response = httpClient.execute(httpGet)
    val entity = response.getEntity
    val reader = new BufferedReader(new InputStreamReader(entity.getContent))
    val line = reader.readLine
    line
  }



}
