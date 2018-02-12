import it.nerdammer.spark.hbase._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}
import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.HTable
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row



object Prueba2Kafka {
  org.apache.log4j.BasicConfigurator.configure()
  def main(args: Array[String]) {
    /** EL código de spark conf para hacer el streaming*/



    val conf = new SparkConf().setAppName("KafkaStreamingHBase")
    if (sys.env("ENTORNO") == "DESARROLLO") {
      conf.setMaster("local[*]")
    }
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

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val rutaTrafico = "file:///C:/Users/plopez/Desktop/events-.1513211291592"
    val trafico = sqlContext.read.json(rutaTrafico)
    val traficoRDD: RDD[(String, Row)] = trafico.selectExpr(List("idTracker", "ip", "url", "parametros.referer", "parametros.evar7", "parametros.evar39", "parametros.evar49", "decay", "useragent", "os", "dispositivo", "language"): _*).rdd.keyBy(t => if (t.getAs[String]("idTracker").indexOf('?') > 0) t.getAs[String]("idTracker").substring(0, t.getAs[String]("idTracker").indexOf('?')) else t.getAs[String]("idTracker"))



    conf.set("spark.hbase.host", "sandbox.hortonworks.com:21000")


    val rdd = sc.parallelize(1 to 100)
      .map(i => (i.toString, i+1, "Hello"))

    rdd.toHBaseTable("b")
      .toColumns("b")
      .inColumnFamily("b")
      .save()




      ssc.start()
      ssc.awaitTermination()


}
}
