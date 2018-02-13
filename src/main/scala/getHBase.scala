import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.ConnectionFactory
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

  val conf = HBaseConfiguration.create()
  conf.set(TableInputFormat.INPUT_TABLE, HBASE_TABLE)

  val connection = ConnectionFactory.createConnection(conf)

  val table = connection.getTable(TableName.valueOf(Bytes.toBytes(HBASE_TABLE)))
  var get = new Get(Bytes.toBytes("b"))
  var result = table.get(get)

  val cells = result.rawCells();

  print(Bytes.toString(result.getRow) + " : ")
  for (cell <- cells) {
    val col_name = Bytes.toString(CellUtil.cloneQualifier(cell))
    val col_value = Bytes.toString(CellUtil.cloneValue(cell))
    val col_timestamp = cell.getTimestamp()
    print("(%s,%s,%s) ".format(col_name, col_value, col_timestamp))
  }

}}