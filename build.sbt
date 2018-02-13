name := "spark"

version := "1.0"

scalaVersion := "2.10.5"

resolvers ++= Seq("ClouderaRepo" at "https://repository.cloudera.com/content/repositories/releases",
                  "Spring" at "http://repo.spring.io/plugins-release/")

val hbaseVersion  = "1.1.2.2.5.0.0-1245"

libraryDependencies ++= Seq(

  "org.apache.hbase" % "hbase-client"  % "1.1.2",
  "org.apache.hbase" % "hbase-common" % "1.1.2",
  "org.apache.hbase" % "hbase-examples" % "1.1.2",
  "zhzhan" % "shc" % "0.0.11-1.6.1-s_2.10",
  "org.apache.hbase" % "hbase-spark" % "1.2.0-cdh5.8.4",
  "org.apache.hbase" % "hbase-annotations" % "1.1.2",
  "org.apache.hbase" % "hbase" % "1.1.2",
  "org.apache.hbase" % "hbase-server" % "1.1.2",
  "org.apache.hbase" % "hbase-protocol" % "1.1.2",
  "org.apache.hbase" % "hbase-hadoop-compat" % "1.1.2",
  "org.apache.htrace" % "htrace-core" % "1.1.2",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.2",
  "org.apache.spark" % "spark-core_2.10" % "1.6.2" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy"),
  "org.apache.spark" % "spark-streaming_2.10" % "1.6.2" exclude ("org.apache.hadoop","hadoop-yarn-server-web-proxy")






).map(_.excludeAll(ExclusionRule(organization = "org.mortbay.jetty"),ExclusionRule(organization = "javax.servlet"))

)