package com.sundogsoftware.sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

import java.util.regex.Pattern
import java.util.regex.Matcher

import Utilities._

import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

object SparkStreaming5 {
  
  def main(args: Array[String]) {
    
    //This is spark streaming program to receive data using Kafka receiver following are the prerequisit
    //Need to start zookeeper for this first
    //2nd step is to start Kafka server
    //create the topic testLogs in case it is not present
    //Broadcast the file on port 9092 using Kafka topic
    //this posted file will be listened and processed by spark streaming file
    
    val ssc = new StreamingContext("local[*]", "Fetching data from Kafka", Seconds(2))
    
    setupLogging()
    
    val pattern = apacheLogPattern()
    
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    
    val topics = List("testLogs").toSet
    
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics).map(_._2)
      
    val requests = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(5)})
    
    val urls = requests.map(x => {val arr = x.toString().split(" "); if (arr.size == 3) arr(1) else "[error]"})
    
    val urlCounts = urls.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(2))
    
    val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    sortedResults.print()
    
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
  
}