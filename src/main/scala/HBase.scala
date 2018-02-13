import org.apache.hadoop.hbase.{HBaseConfiguration,TableName}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._

import org.apache.spark.{SparkContext,SparkConf}

object HBase{
    def main(args: Array[String])  {


    val sparkConf = new SparkConf().setAppName("HBaseDemo")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(sc, conf)

    val rdd =  sc.parallelize(Array(
      (Bytes.toBytes("row1"),
        Array((Bytes.toBytes("colFamily)"), Bytes.toBytes("col1"), Bytes.toBytes("value1"))))))

    rdd.hbaseForeachPartition(hbaseContext, (it, conn) => {
    val bufferedMutator = conn.getBufferedMutator(TableName.valueOf("usuarios"))

    it.foreach( putRecord => {
      val put = new Put(putRecord._1)
      putRecord._2.foreach((putValue) => put.addColumn(putValue._1, putValue._2, putValue._3))
      bufferedMutator.mutate(put)
      })
      bufferedMutator.flush()
      bufferedMutator.close()
    })


  }
}
