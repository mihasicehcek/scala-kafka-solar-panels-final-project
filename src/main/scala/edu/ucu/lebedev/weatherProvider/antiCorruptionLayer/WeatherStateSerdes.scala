package edu.ucu.lebedev.weatherProvider.antiCorruptionLayer

import java.util

import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{read, write}

class WeatherStateSerdes extends Serializer[WeatherState] with Deserializer[WeatherState]{
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
  implicit val formats: DefaultFormats.type = DefaultFormats

  override def serialize(topic: String, data: WeatherState): Array[Byte] = {
    write(data).getBytes
  }

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): WeatherState = {
    read[WeatherState](data.map(_.toChar).mkString)
  }
}
