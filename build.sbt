name := "spark"

version := "1.0"

scalaVersion := "2.10.5"

resolvers += "pentaho-repo" at "https://public.nexus.pentaho.org/content/groups/omni/"

val hbaseVersion  = "1.1.2.2.5.0.0-1245"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.2" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  "org.apache.spark" % "spark-sql_2.10" % "1.6.2" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  "org.apache.hadoop" % "hadoop-common" % "2.7.0" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  "org.apache.spark" % "spark-sql_2.10" % "1.6.2" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  "org.apache.spark" % "spark-hive_2.10" % "1.6.2" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  "org.apache.spark" % "spark-yarn_2.10" % "1.6.2" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.2" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.2" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  "org.apache.hbase"  %  "hbase-common"    % hbaseVersion,
  "org.apache.hbase"  %  "hbase-server"    % hbaseVersion,
  "org.apache.hbase"  %  "hbase-protocol"  % hbaseVersion,
  "org.apache.hbase"  %  "hbase-client"    % hbaseVersion,
  "com.databricks" % "spark-csv_2.10" % "0.1" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy")

).map(_.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"),ExclusionRule(organization = "javax.servlet"))

)


