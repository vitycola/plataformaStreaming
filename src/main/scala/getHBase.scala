import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._

object getHBase{
//object IncrementalJob {
val APP_NAME: String = "SparkHbaseJob"
var HBASE_DB_HOST: String = null
var HBASE_TABLE: String = null
var HBASE_COLUMN_FAMILY: String = null

def main(args: Array[String]) {
  HBASE_DB_HOST = "127.0.0.1"
  HBASE_TABLE = "b"
  HBASE_COLUMN_FAMILY = "b"

  val sparkConf = new SparkConf().setAppName("HBaseget")
  if (sys.env("ENTORNO") == "DESARROLLO") {
    sparkConf.setMaster("local[*]")
  }

  val sc = new SparkContext(sparkConf)
  val rdd = sc.parallelize(Array("b","c")).map(x => x(0))
val rdd1 = rdd.collect()


  val conf = HBaseConfiguration.create()
  conf.set(TableInputFormat.INPUT_TABLE, HBASE_TABLE)

  val connection = ConnectionFactory.createConnection(conf)

  val table = connection.getTable(TableName.valueOf(Bytes.toBytes(HBASE_TABLE)))
  var get = new Get(Bytes.toBytes(rdd1(0)))
  var result = table.get(get)

  val cells = result.rawCells();

  print(Bytes.toString(result.getRow) + " : ")
  for (cell <- cells) {
    val col_name = Bytes.toString(CellUtil.cloneQualifier(cell))
    val col_value = Bytes.toString(CellUtil.cloneValue(cell))
    val col_timestamp = cell.getTimestamp()

  }

}}