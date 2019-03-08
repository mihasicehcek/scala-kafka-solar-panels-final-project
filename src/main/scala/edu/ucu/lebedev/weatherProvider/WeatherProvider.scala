package edu.ucu.lebedev.weatherProvider

import java.util.Properties

import edu.ucu.lebedev.signalProducer.antiCorruptionLayer.{Coordinates, Signal}
import edu.ucu.lebedev.weatherProvider.antiCorruptionLayer.WeatherState
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, Produced}
import org.json4s.DefaultFormats



object WeatherProvider extends App {
  implicit val formats: DefaultFormats.type = DefaultFormats
  org.apache.log4j.BasicConfigurator.configure()

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "weatherProvider")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    p
  }

  val builder = new StreamsBuilder()

  val signals: KStream[Coordinates, Int] = builder.stream[Coordinates, Int]("Weather_Request")(Consumed.`with`(Signal.createSerdesForCoordintes(), Serdes.Integer))

  signals
    .map((k, v) => {
      val weatherState = new WetherApiClient().getWeatherByCoords(k.lat, k.lon)
      (k.lat+":"+k.lon, weatherState.right.get)
    })
    .to("Weater_State")(Produced.`with`(Serdes.String, WeatherState.createSerdes()))


  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.cleanUp()
  streams.start()


}
