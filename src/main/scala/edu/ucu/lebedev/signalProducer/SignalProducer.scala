package edu.ucu.lebedev.signalProducer

import java.util.{Properties, UUID}

import edu.ucu.lebedev.signalProducer.antiCorruptionLayer.{Coordinates, Panel, Signal, SignalSerdes}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.json4s.DefaultFormats

import scala.util.Random


object SignalProducer extends App {
  implicit val formats: DefaultFormats.type = DefaultFormats
  org.apache.log4j.BasicConfigurator.configure()


  val lat = args(0).toDouble
  val lon = args(1).toDouble
  val panelId = UUID.randomUUID().toString

  val producerProps = {
    val p = new Properties()
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[SignalSerdes].getCanonicalName)
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

  val producer = new KafkaProducer[String, Signal](producerProps)
  val rand = Random

  while(true){
    val signal = Signal(rand.nextGaussian(), rand.nextGaussian(), rand.nextDouble(), Panel(panelId, Coordinates(lat, lon)))

    producer.send(new ProducerRecord("Signals", signal.panel.id, signal))

    Thread.sleep(300)
  }

//  producer.close()

}