package TwitterAPI

import java.io.{BufferedWriter, File, FileWriter, IOException, PrintWriter}

import io.circe.Encoder.AsArray.importedAsArrayEncoder
import io.circe.Encoder.AsObject.importedAsObjectEncoder

import scala.io.Source
import cats.implicits
import io.circe.Codec.AsObject
import io.circe._
import io.circe.JsonObject._
import io.circe.syntax._
import io.circe.ObjectEncoder
import io.circe.parser._

//Lots of unused imports here because the plan is to use them eventually.



object Converter {

  def convert(apiresponse: String) = {
//    val response = Source.fromFile("trendsbywoeid/2459115-trends-1605805975016.json")
//
//    for (line <- response.getLines()) {
//      println(line.asJson)
//    }
  println(apiresponse.asJson)

  }

  //case classes and encoders for turning api results into
  //circe readable json objects. We will then parse these
  //to extract the values we want to
  //use in our spark dataframes.
  case class oneTrend ( name: String, url: String, promoted_content: Option[String], query: String, tweet_volume: Option[Int])
  case class fiftyTrends ( trends: Array[oneTrend])
  case class as_of ( asOfTime: String)
  case class created_at ( createTime: String)
  case class place (name: String, woeid: Int)
  case class locations (placeList: Array[place])

  case class bigTuple (tupleTrends: fiftyTrends, tupleAsof: as_of, tupleCreate: created_at, tupleLoc: locations)
  case class Result (apiResult: Array[bigTuple])


  implicit val oneTrendEncoder: Encoder[oneTrend] = aTrend => Json.obj(
    "name" -> aTrend.name.asJson,
    "url" -> aTrend.url.asJson,
    "promoted_content" -> aTrend.promoted_content.asJson,
    "query" -> aTrend.query.asJson,
    "tweet_volume" -> aTrend.tweet_volume.asJson
  )

  implicit val fiftyTrendsEncoder : Encoder[fiftyTrends] = aTrendArray => Json.obj(
    "trends" -> aTrendArray.trends.asJson
  )

  implicit val as_ofEncoder : Encoder[as_of] = aoTime => Json.obj(
    "as_of" -> aoTime.asOfTime.asJson
  )

  implicit val created_atEncoder : Encoder[created_at] = caTime => Json.obj(
    "created_at" -> caTime.createTime.asJson
  )

  implicit val placeEncoder : Encoder[place] = aPlace => Json.obj(
    "name" -> aPlace.name.asJson,
    "woeid" -> aPlace.woeid.asJson
  )

  implicit val locEncoder : Encoder[locations] = someLocs => Json.obj(
    "locations" -> someLocs.placeList.asJson
  )

  implicit val resultEncoder : Encoder[Result] = aResult => Json.obj(
    "trends" -> aResult.apiResult(0).tupleTrends.asJson,
    "as_of" -> aResult.apiResult(0).tupleAsof.asJson,
    "created_at" -> aResult.apiResult(0).tupleCreate.asJson,
    "locations" -> aResult.apiResult(0).tupleLoc.asJson
  )

}
