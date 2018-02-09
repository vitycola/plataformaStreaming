import java.util.Properties
import org.apache.kafka.clients.producer._

object PruebaKafkaProducer extends App {
  org.apache.log4j.BasicConfigurator.configure()
  val  props = new Properties()
  props.put("bootstrap.servers", "51.255.74.114:42111") //Produzco directamente a kafka, no a zookeeper, si se puede en un futuro intento cambiarlo
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer") //Para pasar la Key
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer") //Para pasar el value

  val producer = new KafkaProducer[String, String](props)

  val TOPIC="test" //El topic que vamos a usar

  while(true){ //Bucle de prueba para enviar siempre los mismos datos
    val record = new ProducerRecord(TOPIC, "key", "hello")
    Thread.sleep(5000)
    producer.send(record)
    println("key"+" hello")
  }

  producer.close()
}