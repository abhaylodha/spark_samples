package spark.streaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._
import java.util.concurrent._
import java.util.concurrent.atomic._
import java.lang.Class.Atomic

/**
 * Uses thread-safe counters to keep track of the average length of
 *  Tweets in a stream.
 */
object AverageTweetLength {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Set up a Spark streaming context named "AverageTweetLength" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "AverageTweetLength", Seconds(1))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)

    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText())

    // Map this to tweet character lengths.
    val lengths = statuses.map(status => (status, status.length()))

    // As we could have multiple processes adding into these running totals
    // at the same time, we'll just Java's AtomicLong class to make sure
    // these counters are thread-safe.
    var totalTweets = new AtomicLong(0)
    var totalChars = new AtomicLong(0)
    var longestTweetLength = new AtomicLong(0)
    var shortestTweetLength = new AtomicLong(Long.MaxValue)
    var shortestTweet = new AtomicReference[String]()
    var longestTweet = new AtomicReference[String]()

    // In Spark 1.6+, you  might also look into the mapWithState function, which allows
    // you to safely and efficiently keep track of global state with key/value pairs.
    // We'll do that later in the course.

    lengths.foreachRDD((rdd, time) => {

      var count = rdd.count()
      if (count > 0) {
        totalTweets.getAndAdd(count)

        totalChars.getAndAdd(rdd.map(_._2).reduce((x, y) => x + y))

        val maxLength = rdd.reduce((x, y) => { if (x._2 > y._2) x else y })
        val minLength = rdd.reduce((x, y) => { if (x._2 < y._2) x else y })

        if (shortestTweetLength.get > minLength._2) {
          shortestTweetLength.set(minLength._2)
          shortestTweet = new AtomicReference(minLength._1)
        }

        if (longestTweetLength.get < maxLength._2) {
          longestTweetLength.set(maxLength._2)
          longestTweet = new AtomicReference(maxLength._1)
        }

        println("Total tweets: " + totalTweets.get() +
          " Total characters: " + totalChars.get() +
          " Average: " + totalChars.get() / totalTweets.get() +
          " Longest Tweet Length : " + longestTweetLength.get +
          " Shortest Tweet Length : " + shortestTweetLength.get +
          " Shortest Tweet : " + shortestTweet.get)
      }
    })

    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!
    ssc.checkpoint("E:/temp/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}
