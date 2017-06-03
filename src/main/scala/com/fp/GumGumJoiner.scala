package com.fp

case class Impression(pageViewId : String, numImpressions : Long)

case class AdEvent(pageViewId : String, numViews : Long, numClicks : Long)

case class PageViewStats(pageViewId : String, numImpressions : Long, numViews : Long, numClicks : Long)

/**
  * @note df.groupby().agg() doesn't work like rdd.groupByKey().mapValues(_.sum),
  *       because catalyst optimises it similar to the rdd.reduceByKey() implementation
  *       which reduces localy like a map-reduce combiner at the end of a map phase.
  */

object GumGumJoiner {

  def main(args : Array[String]) : Unit = {

    if (args.length != 3) {
      println("Wrong command line argumnets! Expected: <assets_file> <ad_events_file> <output_file>")
      System exit 1
    }

    val adEventsPath = args(0)
    val assetsPath = args(1)
    val outputPath = args(2)

    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.functions._

    val spark = SparkSession.builder.getOrCreate
    import spark.implicits._

    val pvPattern ="""\"pv\":\"([A-z0-9-]+)\"""".r
    val ePattern = """\"e\":\"(view|click)\"""".r

    // interesting part of the assets file
    // {"pv":"7963ee21-0d09-4924-b315-ced4adad425f"}

    val assets = spark.read
      .textFile(assetsPath)
      .map { line =>
        val pageViewId = pvPattern.findFirstMatchIn(line).get.group(1)
        val numImpressions = 1
        Impression(pageViewId, numImpressions)
      }
      .groupBy("pageViewId")
      .agg(sum('numImpressions) as 'numImpressions)
      .where('numImpressions > 0)

    // interesting part of the adevents file
    // {"e":"block","pv":"98580188-4abd-4a1e-ae98-6e21be2d2c5d"}

    val adEvents = spark.read
      .textFile(adEventsPath)
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

    val pageViewStats = assets
      .join(adEvents, "pageViewId")
      .as[PageViewStats]
      .map {
        case PageViewStats(pageViewId, numImpressions, numViews, numClicks) =>
          s"""$pageViewId\t$numImpressions\t$numViews\t$numClicks"""
      }

    //coalesce(1) may work on small datasets, hadoop - like output with one part -0000
    pageViewStats.coalesce(1).write.text(outputPath)

  }

}
