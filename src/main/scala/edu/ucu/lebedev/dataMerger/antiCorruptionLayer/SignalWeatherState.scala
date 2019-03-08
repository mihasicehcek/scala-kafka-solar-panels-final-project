package edu.ucu.lebedev.dataMerger.antiCorruptionLayer

import java.util

import edu.ucu.lebedev.signalProducer.antiCorruptionLayer.Signal
import edu.ucu.lebedev.weatherProvider.antiCorruptionLayer.WeatherState
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.scala.Serdes
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{read, write}

case class SignalWeatherState(signal: Signal, weather: WeatherState) {}


object SignalWeatherState {
  def createSerdes() : Serde[SignalWeatherState] = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    Serdes.fromFn[SignalWeatherState](
      (obj : SignalWeatherState)  => {write(obj).getBytes},
      (bytes : Array[Byte]) => {
        println(bytes.map(_.toChar).mkString)
        Some(read[SignalWeatherState](bytes.map(_.toChar).mkString))
      }
    )
  }

}
