package com.sundogsoftware.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.twitter._
import org.apache.log4j._
import Utilities._

object SparkStreaming4 {
  
  def main (args: Array[String]) {
    
    //This is a spark streaming program to fetch twitter data and calculate popular hash tag for a given window
    
    setupTwitter()
    
    val ssc = new StreamingContext("local[*]","Popular hash tags", Seconds(2))
    
    setupLogging()
    
    val tweeterData = TwitterUtils.createStream(ssc, None)
    
    val tweets = tweeterData.map(x => x.getText())
    
    val words = tweets.flatMap(x => x.split(" "))
    
    val filtersWords = words.filter(x => x.startsWith("#"))
    
    val data = filtersWords.map(x => (x, 1))
    
    val hashTagCount = data.reduceByKeyAndWindow((x , y) => x+y, (x , y) => x - y, Seconds(300), Seconds(2))
    
    val sortedResult = hashTagCount.transform(rdd => rdd.sortBy(x => x._2, false))
    
    sortedResult.print
    
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
    
    
  }
  
  
}