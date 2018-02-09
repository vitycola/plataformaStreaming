import org.apache.hadoop.hbase.client._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.HTable
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}



object Prueba2Kafka {
  def main(args: Array[String]) {
    /** EL código de spark conf para hacer el streaming*/
    val conf = new SparkConf().setMaster("local[4]").setAppName("ConsumidorKafka")
    val ssc = new StreamingContext(conf, Seconds(5))

    /** KafkaConf tiene un Map de la ruta del server de kafka, la ruta del server de zookeeper, el grupo.id del consumidor para poder hacer redundancia, el timeout para conectar a zookeeper */
    val kafkaConf = Map("metadata.broker.list" -> "localhost:42111",
      "zookeeper.connect" -> "51.255.74.114:21000",
      "group.id" -> "kafka-example",
      "zookeeper.connection.timeout.ms" -> "1000",
      "zookeeper.session.timeout.ms" -> "10000")

    /* Los valores que recibe kafka son 4, un streaming context, el kafkaCOnf, un Map del tópic transformado en integer y el cuarto es como guardar los datos, según este ejemplo solo cojo el value (la key no) */
    val lines = KafkaUtils.createStream[Array[Byte], String, DefaultDecoder, StringDecoder](
      ssc, kafkaConf, Map("test" -> 1),
      StorageLevel.MEMORY_ONLY_SER).map(_._2)

    //** Pasamos a tratar los datos como queramos */
    lines.print()
    print(lines.count())

    lines.foreachRDD ( rdd => {
      val conf = HBaseConfiguration.create()
      conf.set(TableOutputFormat.OUTPUT_TABLE, "b")
      conf.set(TableInputFormat.INPUT_TABLE, "b")
//      HBaseAdmin.checkHBaseAvailable(conf)
      conf.set("hbase.zookeeper.quorum", "51.255.74.114:21000")
//      conf.set("hbase.zookeeper.property.client.port", "21000")
      conf.set("hbase.master", "localhost:60000")
      conf.set("hbase.rootdir", "hdfs://sandbox.hortonworks.com:8020/apps/hbase/data")
//      conf.set("zookeeper.znode.parent", "/hbase-unsecure")

      val connection = ConnectionFactory.createConnection(conf)
      val table = connection.getTable(TableName.valueOf( Bytes.toBytes("b") ) )

      val p = new Put(Bytes.toBytes("b"))
      p.add(Bytes.toBytes("b"), Bytes.toBytes("b"), Bytes.toBytes("b"))
      println("hola2")
      table.put(p)

    })

      ssc.start()
      ssc.awaitTermination()


}
}
