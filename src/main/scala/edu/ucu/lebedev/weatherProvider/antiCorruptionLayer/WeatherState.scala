package edu.ucu.lebedev.weatherProvider.antiCorruptionLayer

import java.util

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.scala.Serdes
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{read, write}

case class WeatherState(main: Main, coord: Coordinates, wind: Wind, dt: Int, sys: Option[System]) {}

case class Main(
                 temp : Option[Float],
                 pressure : Option[Float],
                 humidity : Option[Float],
                 temp_min : Option[Float],
                 temp_max : Option[Float]
               )
case class Coordinates(lon: Float, lat: Float)
case class System(message: Option[Float], country: String, sunrise: Int, sunset: Int)
case class Wind(speed: Option[Float], deg: Option[Float])


object WeatherState {
  def createSerdes() : Serde[WeatherState] = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    Serdes.fromFn[WeatherState](
      (obj : WeatherState)  => {write(obj).getBytes},
      (bytes : Array[Byte]) => {Some(read[WeatherState](bytes.map(_.toChar).mkString))}
    )
  }

}

