package com.sundogsoftware.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object firstSparkProgram {
  
  def splitData(line:String) = {
    
    val data = line.split("\t")
    val rating = data(2).toString()
    rating
  }
  
  def main(args: Array[String]){
    //This is a spark program to calculate the count of ratings in u.data file
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "firstSparkProgram")
    
    val lines = sc.textFile("D:/Bigdata_Data/ml-100k/u.data")
    
    val ratings = lines.map(splitData)
    
    //val ratingMap = ratings.map(x => (x,1))
    //val count = ratingMap.reduceByKey((x,y) => (x+y))
    
    val count = ratings.countByValue()
    
    //val result = count.sortByKey()
    val result = count.toSeq.sortBy(_._1)
    
    for (i <- result) {
      println(i._1, i._2)
    }
    
    
  }
   
}