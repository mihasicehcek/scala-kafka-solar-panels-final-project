package edu.ucu.lebedev.dataMerger

import java.util.Properties

import edu.ucu.lebedev.dataMerger.antiCorruptionLayer.SignalWeatherState
import edu.ucu.lebedev.signalProducer.antiCorruptionLayer.Signal
import edu.ucu.lebedev.weatherProvider.antiCorruptionLayer.WeatherState
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream.{Consumed, Joined, KStream, KTable}


object DataMergingWorker extends App {
  org.apache.log4j.BasicConfigurator.configure()

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "assignment-2-aggregate")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    p
  }

  val builder = new StreamsBuilder()

  val weater: KTable[String, WeatherState] = builder
    .table[String, WeatherState]("Weater_State")(Consumed.`with`(Serdes.String, WeatherState.createSerdes()))

  builder.stream[String, Signal]("Signals")(Consumed.`with`(Serdes.String, Signal.createSerdes()))
    .map((id : String, signal: Signal) => {
      (signal.panel.coords.lat+":"+signal.panel.coords.lon, signal)
    })
    .leftJoin(weater)((s:Signal, w:WeatherState) => {
      SignalWeatherState(s, w)
    })(Joined.`with`(Serdes.String, Signal.createSerdes(), WeatherState.createSerdes()))
    .to("Signal_With_Weather")(Produced.`with`(Serdes.String, SignalWeatherState.createSerdes()))

  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.cleanUp()
  streams.start()

}
