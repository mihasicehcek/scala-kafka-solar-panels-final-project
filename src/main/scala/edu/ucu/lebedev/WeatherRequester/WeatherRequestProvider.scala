package edu.ucu.lebedev.WeatherRequester

import java.time.Duration
import java.util
import java.util.{Properties, UUID}

import com.typesafe.config.{Config, ConfigFactory}
import edu.ucu.lebedev.signalProducer.antiCorruptionLayer.{Coordinates, CoordinatesSerdes, Signal, SignalSerdes}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{IntegerSerializer, StringDeserializer}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.json4s.DefaultFormats

import scala.collection.JavaConverters._

class WeatherRequestProvider {

}

object WeatherRequestProvider extends App {
  implicit val formats: DefaultFormats.type = DefaultFormats
  org.apache.log4j.BasicConfigurator.configure()
  val config: Config = ConfigFactory.load("app.conf")

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "weatherRequestProvider")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.socket"))

    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[SignalSerdes].getCanonicalName)
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "weatherRequestProvider")

    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[CoordinatesSerdes].getCanonicalName)
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[IntegerSerializer].getCanonicalName)
    p
  }

  val consumer = new KafkaConsumer[String, Signal](props)
  consumer.subscribe(Seq("Signals").asJava)

  val producer = new KafkaProducer[Coordinates, Int](props)

  while(true){
    val records : ConsumerRecords[String, Signal] = consumer.poll(Duration.ofSeconds(1))

    records.iterator().asScala.toList
      .map(record => {
        record.value().panel.coords
      })
      .groupBy(cords => {cords.lon+"_"+cords.lat})
      .map(record => {
        (record._2.head, record._2.size)
      })
      .foreach(v => {
        producer.send(new ProducerRecord[Coordinates, Int]("Weather_Request", v._1, v._2))
      })

    Thread.sleep(10*60*1000)
  }

}
