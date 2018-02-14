import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.spark.HBaseDStreamFunctions._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.{SparkContext, Logging}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._



object HBaseStream{
   def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("HBaseStream")
    if (sys.env("ENTORNO") == "DESARROLLO") {
      sparkConf.setMaster("local[*]")
    }

    sparkConf.set("spark.hbase.host", "51.255.74.114:16000")
    val sc = new SparkContext(sparkConf)

     sc.setLogLevel("ERROR")
     val config = HBaseConfiguration.create()
     val hbaseContext = new HBaseContext(sc, config)
     val ssc = new StreamingContext(sc, Seconds(10))
    ssc.checkpoint("/tmp/spark_checkpoint")


    val lines = KafkaUtils.createStream(ssc, "51.255.74.114:21000", "plataformaStreaming", Map(("streamingSpark", 1))).map(_._2)
    val csv = lines.map(x => x.split(","))
    //144,ALICANTE,20170117,4.8,7.2
    csv.hbaseBulkPut(
      hbaseContext,
      TableName.valueOf("usuarios"),
      (putRecord) => {
        val put = new Put(Bytes.toBytes(putRecord(0)))
        put.addColumn(Bytes.toBytes("adn"), Bytes.toBytes("provincia"), Bytes.toBytes(putRecord(1)))
        put.addColumn(Bytes.toBytes("adn"), Bytes.toBytes("fecha"), Bytes.toBytes(putRecord(2)))
        put.addColumn(Bytes.toBytes("adn"), Bytes.toBytes("precipitacion"), Bytes.toBytes(putRecord(3)))
        put.addColumn(Bytes.toBytes("adn"), Bytes.toBytes("temperatura"), Bytes.toBytes(putRecord(4)))
        put
      })
    ssc.start()
    ssc.awaitTermination()

  }}
