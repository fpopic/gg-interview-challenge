package com.fp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class GumGumJoinerTest extends FlatSpec with BeforeAndAfter with Matchers {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark : SparkSession = SparkSession.builder
    .master("local[*]")
    .config("spark.driver.allowMultipleContexts", "true")
    .getOrCreate

  before {
    spark.newSession()
  }

  after {
    spark.stop()
    System.clearProperty("spark.driver.port")
  }

  "Main test" should "return return one impression, zero views and one click." in {
    import spark.implicits._

    val assetLineWithClickAndJoinMatch = """Mon Jan 20 00:51:45 -0800 2014, {"cl":"js","ua":"Mozilla/5.0 (iPhone; CPU iPhone OS 7_0_4 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) Version/7.0 Mobile/11B554a Safari/9537.53","ip":"108.23.175.236","cc":"US","rg":"CA","ct":"Upland","pc":"91786","mc":803,"bf":"05f5327043c9c1b3f9403fc788f9aee8eb77d66d","lt":"Mon Jan 20 00:51:45 -0800 2014","hk":["belly","blast","hula hooping","fat","ways","fun","hula hoops","unique"]}, {"v":"1.1","pv":"eeeba52d-8f8b-4a79-8e76-649ce9376619","r":"v3","t":"b465729b","a":[{"i":1,"u":"www.activebeat.com/wp-content/uploads/2013/03/shutterstock_72360298.jpg","w":300,"h":231,"x":10,"y":571,"lt":"none","af":false,"it":"Hula Hooping"}],"rf":"http://m.activebeat.com/fitness/10-fun-and-unique-ways-to-blast-belly-fat/2/","p":"m.activebeat.com/fitness/10-fun-and-unique-ways-to-blast-belly-fat/3/","fs":false,"tr":0.2,"ac":{},"vp":{"ii":false,"w":320,"h":372},"sc":{"w":320,"h":480,"d":2},"pid":11142,"vid":14}"""
    val adEventLineWithClickAndJoinMatch = """Mon Jan 20 00:51:55 -0800 2014, {"cl":"js","ua":"Mozilla/5.0 (iPhone; CPU iPhone OS 7_0_4 like Mac OS X) AppleWebKit/537.51.1 (KHTML, like Gecko) Version/7.0 Mobile/11B554a Safari/9537.53","ip":"108.23.175.236","cc":"US","rg":"CA","ct":"Upland","pc":"91786","mc":803,"bf":"05f5327043c9c1b3f9403fc788f9aee8eb77d66d","lt":"Mon Jan 20 00:51:45 -0800 2014"}, {"v":"1.1","e":"click","et":"valid","t":"b465729b","ai":"","ab":22617,"u":"http://www.activebeat.com/wp-content/uploads/2013/03/shutterstock_72360298.jpg","seq":1,"tr":0.2,"af":false,"pv":"eeeba52d-8f8b-4a79-8e76-649ce9376619","pu":"http://m.activebeat.com/fitness/10-fun-and-unique-ways-to-blast-belly-fat/3/","rpc":0.22,"pc":0,"du":"http://townrant.com/profile/1870602973","nc":0,"cid":44}"""

    val pvPattern ="""\"pv\":\"([A-z0-9-]+)\"""".r
    val ePattern = """\"e\":\"(view|click)\"""".r

    val assets = spark
      .createDataset(Seq(assetLineWithClickAndJoinMatch))
      .map { line =>
        val pageViewId = pvPattern.findFirstMatchIn(line).get.group(1)
        val numImpressions = 1
        Impression(pageViewId, numImpressions)
      }
      .groupBy("pageViewId")
      .agg(sum('numImpressions) as 'numImpressions)
      .where('numImpressions > 0)

    val adEvents = spark
      .createDataset(Seq(adEventLineWithClickAndJoinMatch))
      .filter(ePattern.findFirstIn(_).isDefined)
      .map { line =>
        val pageViewId = pvPattern.findFirstMatchIn(line).get.group(1)
        val eventType = ePattern.findFirstMatchIn(line).get.group(1)
        val numViews = if (eventType == "view") 1 else 0
        val numClicks = 1 - numViews
        AdEvent(pageViewId, numViews, numClicks)
      }
      .groupBy("pageViewId")
      .agg(sum('numViews) as 'numViews, sum('numClicks) as 'numClicks)

    val expectedOutput = s"""eeeba52d-8f8b-4a79-8e76-649ce9376619\t${1}\t${0}\t${1}"""
    val actualOutput = assets
      .join(adEvents, "pageViewId")
      .as[PageViewStats]
      .map {
        case PageViewStats(pageViewId, numImpressions, numViews, numClicks) =>
          s"""$pageViewId\t$numImpressions\t$numViews\t$numClicks"""
      }
      .first()

    actualOutput shouldEqual expectedOutput
  }

}