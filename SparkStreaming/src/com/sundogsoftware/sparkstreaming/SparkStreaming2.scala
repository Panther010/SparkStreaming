package com.sundogsoftware.sparkstreaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j._
import Utilities._


object SparkStreaming2 {
  
  def main(args: Array[String]){
    
    setupTwitter()
    
    val ssc = new StreamingContext("local[*]","SaveTweets", Seconds(5))
    
    setupLogging()
    
    val twitterData = TwitterUtils.createStream(ssc, None)
    
    val tweets = twitterData.map(x => x.getText())
    
    var tweetsCount:Long = 0
    
    tweets.foreachRDD((rdd, time) => {
      
      if (rdd.count() > 0) {
        val repartitionTweets = rdd.repartition(1).cache()
        
        repartitionTweets.saveAsTextFile("Tweets_" + time.milliseconds.toString)
        
        tweetsCount += repartitionTweets.count()
        println("Tweet count: " + tweetsCount)
        if (tweetsCount > 1000){
          System.exit(0)
        }
      } 
    })
    
    ssc.start()
    ssc.awaitTermination()
    
  }
  
}