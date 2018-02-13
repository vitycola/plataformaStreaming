//import it.nerdammer.spark.hbase._
//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf
//
///**
//  * Created by hkropp on 19/04/15.
//  */
//object HBase{
//  //  org.apache.log4j.BasicConfigurator.configure()
//  def main(args: Array[String])  {
//
//    val sparkConf = new SparkConf().setAppName("SanitasSpark")
//    if (sys.env("ENTORNO") == "DESARROLLO") {
//      sparkConf.setMaster("local[*]")
//    }
//
//    sparkConf.set("spark.hbase.host", "sandbox.hortonworks.com:21000")
//    val sc = new SparkContext(sparkConf)
//
//    val rdd = sc.parallelize(1 to 100)
//      .map(i => (i.toString, i+1, "Hello"))
//
//    rdd.toHBaseTable("b")
//      .toColumns("b")
//      .inColumnFamily("b")
//      .save()
//
//
//
//  }
//}
