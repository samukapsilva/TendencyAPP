package br.com.tendencyapp.populartagsController

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
//import br.com.tendencyapp.streamingLog.StreamingLog

object TwitterPopularTags{
  
  def main(args: Array[String]){
    println("Iniciando buscas")
    
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

   val sparkConf = new SparkConf().setAppName("TendencyAPP").setMaster("local[2]")
   val scc = new StreamingContext(sparkConf, Seconds(2))
   val stream = TwitterUtils.createStream(scc, None, filters)
   
   val hashTags = stream.flatMap(status => status.getText.split(" ").filter( _.startsWith("#")))
   
   val topCounts60 = hashTags.map((_,1)).reduceByKeyAndWindow(_ + _, Seconds(60))
                      .map{case( topic, count) => (count, topic)}
                      .transform(_.sortByKey(false))
                      
   val topCounts10 = hashTags.map((_,1)).reduceByKeyAndWindow(_ + _, Seconds(10))
                      .map{case( topic, count) => (count, topic)}
                      .transform(_.sortByKey(false))                      
   
   // print popular hashTags

   topCounts60.foreachRDD(rdd => {
     val topList = rdd.take(10)
     println("\nPopular topics in last minute (%s total):".format(rdd.count()))
     topList.foreach{case (count, tag ) => println("%s"("%s tweets").format(tag, count))}
   })
   
   topCounts10.foreachRDD(rdd => {
     val topList = rdd.take(10)
     println("\nPopular topics in last minute (%s total):".format(rdd.count()))
     topList.foreach{case (count, tag ) => println("%s"("%s tweets").format(tag, count))}
   })
   
   scc.start()
   scc.awaitTermination()
   
  }
}