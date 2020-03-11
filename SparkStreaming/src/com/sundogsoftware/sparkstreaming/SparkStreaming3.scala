package com.sundogsoftware.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.twitter._
import org.apache.log4j._
import Utilities._
import java.util.concurrent._
import java.util.concurrent.atomic._

object SparkStreaming3 {
  
  def main(args: Array[String]){
    
    //This spark streaming program fetch twitter data and print average length of tweets
    
    setupTwitter()
    
    val ssc = new StreamingContext("local[*]", "Avg length of tweet", Seconds(2))
    
    setupLogging()
    
    val twiterData = TwitterUtils.createStream(ssc, None)
    
    val tweets = twiterData.map(x => x.getText())
    val tweetsLength = tweets.map(x => x.length())
    
    var totalTweet = new AtomicLong(0)
    var totalLength = new AtomicLong(0)
    
    tweetsLength.foreachRDD((rdd, time) => {
      
      var count = rdd.count()
       if (count > 0) {
         totalTweet.getAndAdd(count)
         totalLength.getAndAdd(rdd.reduce((x, y) => x+y))
         
         println("Total Tweets: " + totalTweet.get() + " *Total character: " + totalLength.get() + " *Average : " + totalLength.get()/totalTweet.get())
       }
      
  })
    ssc.start()
    ssc.awaitTermination()
  }
  
}