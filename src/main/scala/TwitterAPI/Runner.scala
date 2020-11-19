package TwitterAPI

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.nio.file.{Files, Paths}

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

    //Using Bearer Token to authenticate api requests
    //Make this variable name ("TWITTER_BEARER_TOKEN") match yours
    val bearer_token = System.getenv("TWITTER_BEARER_TOKEN")
    println(s"Authenticating with Bearer Token: ${bearer_token}")

    //Ask user for woeid to use as query input
    var location = StdIn.readLine("Please enter a WOEID. By default, New York City (capital of the Galaxy) is used." +
      "Enter '1' to pull global trends.\n WOEID: ")


    //

    //Sending get request to API and adding trends to directory
    Future {
      trendsToDir(bearer_token,location)
      println("Finished pulling trends.")
    }
    println("Beginning to collect trends into directory 'trendsbywoeid'. ")

    //creating spark session
    val sesh = SparkSession.builder()
      .appName("Twitter API Program")
      .master("local[4]")
      .getOrCreate()
    import sesh.implicits._
    sesh.sparkContext.setLogLevel("WARN")

    //static dataframe (maybe change to dataset?)
    //val staticDF = sesh.read.json("Geo-trend-results")

  }
    def trendsToDir (authToken: String, woeid: String="2459115", dirName: String="trendsbywoeid"): Unit = {
      val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build
      val uriBuilder = new URIBuilder(s"https://api.twitter.com/1.1/trends/place.json?id=${woeid}")
      val httpGet = new HttpGet(uriBuilder.build)
      httpGet.setHeader("Authorization", String.format("Bearer %s", authToken))
      val response = httpClient.execute(httpGet)
      val entity = response.getEntity
      if (null != entity) {
        val reader = new BufferedReader(new InputStreamReader(entity.getContent))
        var line = reader.readLine
        var fileWriter = new PrintWriter(Paths.get("geotrends.tmp").toFile)
        var lineNumber = 1
        val millis = System.currentTimeMillis()
        while ( {
          line != null
        }) {
          fileWriter.println(line)
          line = reader.readLine()
        }
        fileWriter.close()
        Files.move(
          Paths.get("geotrends.tmp"),
          Paths.get(s"${dirName}/${woeid}-trends-${millis}.json")
        )
        fileWriter = new PrintWriter(Paths.get("geotrends.tmp").toFile)


      }
    }

//  def trendsToDir (authToken: String, woeid: String="2459115", dirName: String="trendsbywoeid", linesPerFile: Int=1000): Unit = {
//    val httpClient = HttpClients.custom.setDefaultRequestConfig(RequestConfig.custom.setCookieSpec(CookieSpecs.STANDARD).build).build
//    val uriBuilder = new URIBuilder(s"https://api.twitter.com/1.1/trends/place.json?id=${woeid}")
//    val httpGet = new HttpGet(uriBuilder.build)
//    httpGet.setHeader("Authorization", String.format("Bearer %s", authToken))
//    val response = httpClient.execute(httpGet)
//    val entity = response.getEntity
//    if (null != entity) {
//      val reader = new BufferedReader(new InputStreamReader(entity.getContent))
//      var line = reader.readLine
//      var fileWriter = new PrintWriter(Paths.get("geotrends.tmp").toFile)
//      var lineNumber = 1
//      val millis = System.currentTimeMillis()
//      while ( {
//        line != null
//      }) {
//        if (lineNumber % linesPerFile == 0) {
//          fileWriter.close()
//          Files.move(
//            Paths.get("geotrends.tmp"),
//            Paths.get(s"${dirName}/${woeid}-trends-${millis}")
//          )
//          fileWriter = new PrintWriter(Paths.get("geotrends.tmp").toFile)
//        }
//        fileWriter.println(line)
//        line = reader.readLine()
//        lineNumber += 1
//      }
//    }
//  }

}
