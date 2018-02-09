import java.util.Properties

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
  * Created by hkropp on 19/04/15.
  */
object HBase{
  org.apache.log4j.BasicConfigurator.configure()
  def main(args: Array[String])  {


      val conf = HBaseConfiguration.create()
      conf.set(TableOutputFormat.OUTPUT_TABLE, "b")
      conf.set(TableInputFormat.INPUT_TABLE, "b")
      //      HBaseAdmin.checkHBaseAvailable(conf)
      conf.set("hbase.zookeeper.quorum", "51.255.74.114:21000")
      //      conf.set("hbase.zookeeper.property.client.port", "21000")
      conf.set("hbase.master", "51.255.74.114:60000")
      conf.set("hbase.rootdir", "hdfs://sandbox.hortonworks.com:8020/apps/hbase/data")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")


      val connection = ConnectionFactory.createConnection(conf)
      val table = connection.getTable(TableName.valueOf( Bytes.toBytes("b") ) )

      val p = new Put(Bytes.toBytes("b"))
      p.add(Bytes.toBytes("b"), Bytes.toBytes("b"), Bytes.toBytes("b"))
      println("hola2")
      table.put(p)
    println("hola3")
  }
}