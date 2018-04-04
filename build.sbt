name := "TendencyApp"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(  
  "org.apache.spark" 	% "spark-core_2.11" % "2.1.1",
  "org.apache.spark" 	% "spark-sql_2.10" % "2.1.1",
  "org.apache.spark" 	% "spark-streaming_2.10" % "2.1.1",
  "org.apache.bahir" 	% "spark-streaming-twitter" % "2.0.1",
  "org.apache.spark" 	% "spark-streaming-kafka_2.10" % "1.1.0",
  "org.apache.spark" 	% "spark-mllib_2.11" % "2.1.1",
  "org.apache.spark" 	% "spark-hive_2.10" % "2.1.1",
  "org.apache.spark" 	% "spark-yarn_2.10" % "2.1.1",
  "org.apache.spark" 	% "spark-graphx_2.10" % "2.1.1",
  "org.apache.spark" 	% "spark-streaming-twitter_2.10" % "1.6.3",
  "com.datastax.spark"  % "spark-cassandra-connector_2.10" % "2.0.1",
  "com.databricks" 		% "spark-csv_2.10" % "1.5.0"
   )