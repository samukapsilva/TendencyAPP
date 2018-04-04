package br.com.tendencyapp.sentimentAnalises

import org.apache.spark.streaming.{Seconds,StreamingContext}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.rdd._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.storage.StorageLevel
import scala.io.Source
import scala.collection.mutable.HashMap
import java.io.File
//import br.com.tendencyapp.streamingLog.StreamingLog



object mapr{
  
    def main(args: Array[String]): Unit = {
        if(args.length <4 ){
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
          "<access token> <access token secret> [<filters>]")
          System.exit(1)
    }
    
      //StreamingLog.setStreamingLogLevels()
   
   val Array (consumerKey, consumerSecret, accesstoken, accessTokenSecret) = args.take(4)
   val filters = args.takeRight(args.length -4)
   
      // Set the system properties so that twitter4j  used by twitter stream can use to generate OAuth credentials
   System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
   System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
   System.setProperty("twitter4j.oauth.accessToken", accesstoken)
   System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

   val sparkConf = new SparkConf().setAppName("TendencyAppSentiments").setMaster("local[2]")
   val ssc = new StreamingContext(sparkConf, Seconds(5))
   val stream = TwitterUtils.createStream(ssc, None, filters)
   
   val tags = stream.flatMap{ status => status.getHashtagEntities.map(_.getText())}
   
      //RDD transformation using sortBy and then map function
     tags.countByValue().foreachRDD {rdd => val now = org.joda.time.DateTime.now()
     rdd
     .sortBy(_._2)
     .map(x => (x, now))
     //saving our output ~/twiteer/directory
     .saveAsTextFile(s"~/twitter/$now")
   }
     
  //DStream transformation using filter and map functions
  val tweets = stream.filter{t =>
    val tags = t.getText.split("").filter(_.startsWith("#")).map(_.toLowerCase)
    tags.exists{ x => true}
  }
  
  val data = tweets.map {status => 
  val sentiment = SentimentAnalysisUtils.detectSentiment(status.getText)
  val tagss = status.getHashtagEntities.map(_.getText.toLowerCase)
  (status.getText,sentiment.toString, tagss.toString())
  }
  
  data.print()
  
  //Saving our output at ~/ with filenames starting like twiterss
  data.sabeAstextFiles("~/twitterss", "20000")
  
  ssc.start()
  ssc.awaitTermination()
  
  }
}