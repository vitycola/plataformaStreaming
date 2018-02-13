import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}
import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object Prueba2Kafka {
//  org.apache.log4j.BasicConfigurator.configure()
  def main(args: Array[String]):Unit = {
    /** EL código de spark conf para hacer el streaming*/
    val conf = new SparkConf().setMaster("local[4]").setAppName("ConsumidorKafka")
    val ssc = new StreamingContext(conf, Seconds(5))

    /* KafkaConf tiene un Map de la ruta del server de kafka, la ruta del server de zookeeper, el grupo.id del consumidor para poder hacer redundancia, el timeout para conectar a zookeeper */
    val kafkaConf = Map("metadata.broker.list" -> "localhost:42111",
      "zookeeper.connect" -> "51.255.74.114:21000",
      "group.id" -> "kafka-example",
      "zookeeper.connection.timeout.ms" -> "1000",
      "zookeeper.session.timeout.ms" -> "10000")

    /* Los valores que recibe kafka son 4, un streaming context, el kafkaCOnf, un Map del tópic transformado en integer y el cuarto es como guardar los datos, según este ejemplo solo cojo el value (la key no) */
    val lines = KafkaUtils.createStream[Array[Byte], String, DefaultDecoder, StringDecoder](
      ssc, kafkaConf, Map("test" -> 1),
      StorageLevel.MEMORY_ONLY_SER).map(_._2)
    //    lines.count().print()


    /* Creamos el streamSqlContext para usar Json */
    val sc = ssc.sparkContext
    val streamSqlContext = new org.apache.spark.sql.SQLContext(sc)
    import streamSqlContext._

    /*Importamos taxonomías*/
    val rutaTax = "file:///C:/Users/plopez/Desktop/Taxonomias.csv" //URL a identificador
    val camposTax = "file:///C:/Users/plopez/Desktop/DictTax.csv" //identificador a taxoniomía
    val camposGenerales = "file:///C:/Users/plopez/Desktop/dictVarSanitas.txt"  //Campos que contiene el Json recibido
//    val rutaTrafico = args(3)    No se usa, la ruta es lo que leemos de kafka
//    val destino = args(4)  Es HBase, aún desconocemos que meteremos al final

    val tax = sc.textFile(rutaTax,1)
    val taxFM = tax.map(x=>(x.split(";")(0),x.split(";")(1)))
    val camposTaxRDD = sc.textFile(camposTax,1)
    val camposGeneralesRDD = sc.textFile(camposGenerales,1)

    val mapaTax = camposTaxRDD.map(x=>(x.split(";")(0),0)).collect()
    val mapaVarGen = camposGeneralesRDD.map(x=>(x.replaceAll("parametros.",""),"")).collect()

    mapaTax.foreach(println)

lines.print()

     lines.foreachRDD { k =>
      if (k.count() > 0) {
        /*Traducimos el Json a RDD */
        val traficoRDD: RDD[(String, Row)] = streamSqlContext.read.json(k).selectExpr(List("idTracker", "url")++camposGeneralesRDD.collect(): _*).rdd.keyBy(t => if (t.getAs[String]("idTracker").indexOf('?') > 0) t.getAs[String]("idTracker").substring(0, t.getAs[String]("idTracker").indexOf('?')) else t.getAs[String]("idTracker"))
//        traficoRDD.collect().foreach(print)

        var traficoTax = traficoRDD.join(taxFM).map(x=>(x._2._1.getAs("idTracker").toString(),x._2))
        var traficoTaxString = traficoTax.groupByKey(4).map(x=>sumatorio(x,mapaTax,mapaVarGen))

        val resultado = anadirCabecera(sc,mapaTax,mapaVarGen,traficoTaxString)
        resultado.collect().foreach(k=>println("resultado: "+k))
//        Utilidades.guardarRddTextFile(resultado,"file:///C:/Users/plopez/Desktop/resultado.csv")


        /* HBase */

        conf.set("spark.hbase.host", "sandbox.hortonworks.com:21000")
        val rdd = sc.parallelize(1 to 100)
          .map(i => (i.toString, i+1, "Hello"))


      }
    }






      ssc.start()
      ssc.awaitTermination()
}
  def sumatorio(info:(String, Iterable[(Row,String)]),mapaTax:Array[(String,Int)],mapaGen:Array[(String,String)]):String = {
    var mapaTaxAux = collection.mutable.Map() ++ mapaTax.clone()
    var mapaGenAux = collection.mutable.Map() ++ mapaGen.clone()

    for(elem <- info._2) {
      mapaTaxAux(elem._2)+=1
      for((varGen,v) <- mapaGen) {
        try{
          val valor = elem._1.getAs(varGen).toString.replaceAll("undefined","")
          if (!(mapaGenAux(varGen) != "" && valor =="")){
            if(varGen == "ip"){
              mapaGenAux(varGen) = valor.split(",")(valor.split(",").size-1).trim()
            } else {
              mapaGenAux(varGen) = valor.split(";")(0)
            }

          }

        } catch {
          case e: Exception => mapaGenAux(varGen)="";
        }
      }
    }

    var resultadoTax = ""
    for ((k,v)<-mapaTax){
      resultadoTax += mapaTaxAux(k) + ";"
    }
    for ((k,v)<-mapaGen){
      resultadoTax += mapaGenAux(k) + ";"
    }
    return info._1+";"+resultadoTax.substring(0,resultadoTax.length-1)
  }

  def anadirCabecera(sc:SparkContext, mapa:Array[(String,Int)],mapaGen:Array[(String,String)], rdd: RDD[String]):RDD[String] = {

    var columnas = ""
    for ((k,v)<-mapa){
      columnas += "idTax"+k + ";"
    }

    for ((k,v)<-mapaGen){
      columnas += k + ";"
    }
    columnas = columnas.replaceAll(";language;",";languageNav;");
    return sc.parallelize(List("idTracker;"+columnas.substring(0,columnas.length-1)),1).union(rdd)
  }
}
