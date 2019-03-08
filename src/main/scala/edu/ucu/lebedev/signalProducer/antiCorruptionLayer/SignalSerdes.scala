package edu.ucu.lebedev.signalProducer.antiCorruptionLayer

import java.util

import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{read, write}

class SignalSerdes  extends Serializer[Signal] with Deserializer[Signal]{
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
  implicit val formats: DefaultFormats.type = DefaultFormats

  override def serialize(topic: String, data: Signal): Array[Byte] = {
    write(data).getBytes
  }

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): Signal = {
    read[Signal](data.map(_.toChar).mkString)
  }
}
