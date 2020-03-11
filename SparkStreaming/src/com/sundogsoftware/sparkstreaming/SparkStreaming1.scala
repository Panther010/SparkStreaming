package com.sundogsoftware.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j._
import Utilities._


object SparkStreaming1 {
  
  def main(args: Array[String]){
    
    setupTwitter()
    
    val ssc = new StreamingContext("local[*]","PrintTweets", Seconds(1))
    
    setupLogging()
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val twitterData = TwitterUtils.createStream(ssc, None)
    
    val tweets = twitterData.map(x => x.getText())
    
    tweets.print()
    ssc.start()
    ssc.awaitTermination()
  }
  
}