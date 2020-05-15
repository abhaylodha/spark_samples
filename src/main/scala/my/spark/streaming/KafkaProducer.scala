package my.spark.streaming

import java.util.{ Date, Properties }

import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }

import scala.util.Random
import java.text.SimpleDateFormat

/**
 * scala -cp F:\Workspaces\Scala\Sample_Spark\build\libs\Sample_Spark.jar my.spark.streaming.KafkaProducer 100 test localhost:9092
 */
object KafkaProducer {
  def main(args: Array[String]) = {
    val events = args(0).toInt
    val topic = args(1)
    val brokers = args(2)
    val rnd = new Random()
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", "ScalaProducerExample")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val t = System.currentTimeMillis()
    for (event_no <- Range(0, events)) {
      val runtime = new Date().getTime()
      val formatter = new SimpleDateFormat("HH:mm:ss.SSS")
      val dateFormatted = formatter.format(runtime)

      val temperature = rnd.nextInt(50)
      val msg = dateFormatted + "," + event_no + "," + temperature
      println("Message : " + msg)
      val data = new ProducerRecord[String, String](topic, msg, msg)

      producer.send(data)
      Thread.sleep(500L)

    }

    System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
    producer.close()
  }
}
